import {
  OrganizationsClient,
  ListAccountsCommand,
} from "@aws-sdk/client-organizations";
import {
  SSOAdminClient,
  ListInstancesCommand,
  ListPermissionSetsCommand,
  DescribePermissionSetCommand,
  ListAccountAssignmentsCommand,
} from "@aws-sdk/client-sso-admin";
import {
  IdentitystoreClient,
  DescribeUserCommand,
  DescribeGroupCommand,
  ListGroupMembershipsCommand,
} from "@aws-sdk/client-identitystore";
import * as cliProgress from "cli-progress";
import chalk from "chalk";
import pLimit from "p-limit";
import logUpdate from "log-update";
import * as fs from "fs";
import * as path from "path";

// ============================================================================
// CONFIGURATION - Adjust these values based on your needs
// ============================================================================

// AWS Region: Change to match your SSO instance region
const AWS_REGION = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || "us-west-2";

// Concurrency: Number of parallel API requests
// - Start with 10 (conservative, works for most accounts)
// - If you see rate limit warnings, decrease to 5
// - If no issues and want faster, try 15-20
// - AWS SSO typically allows 20 TPS (transactions per second)
const CONCURRENCY_LIMIT = 10;

// Retry configuration for rate limiting
const MAX_RETRIES = 5;
const INITIAL_RETRY_DELAY = 1000; // milliseconds (1 second)

// Output file configuration
const OUTPUT_FILE = "sso-assignments.csv";

// ============================================================================

interface AdminAssignment {
  accountNumber: string;
  accountName: string;
  username: string;
  permissionSetName: string;
  assignmentType: "USER" | "GROUP";
  groupName?: string;
}

// Caches to reduce API calls
const userCache = new Map<string, string>();
const groupCache = new Map<string, string>();
const groupMembersCache = new Map<string, string[]>();

// Global logger function (will be set in main)
let globalLogger: ((message: string, type?: "info" | "success" | "warning") => void) | null = null;

// Exponential backoff with jitter
async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  operationName: string,
  retries = MAX_RETRIES
): Promise<T> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      const isRateLimitError =
        error.name === "TooManyRequestsException" ||
        error.name === "ThrottlingException" ||
        error.$metadata?.httpStatusCode === 429;

      // If it's not a rate limit error or we're out of retries, throw
      if (!isRateLimitError || attempt === retries) {
        throw error;
      }

      // Calculate exponential backoff with jitter
      // delay = base * 2^attempt + random jitter
      const exponentialDelay = INITIAL_RETRY_DELAY * Math.pow(2, attempt);
      const jitter = Math.random() * 1000; // 0-1000ms random jitter
      const delay = exponentialDelay + jitter;

      const message = `Rate limit: ${operationName}. Retry in ${Math.round(
        delay / 1000
      )}s (${attempt + 1}/${retries})`;

      if (globalLogger) {
        globalLogger(message, "warning");
      }

      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  throw new Error(`Failed after ${retries} retries`);
}

async function getOrgAccounts(
  orgClient: OrganizationsClient
): Promise<Map<string, string>> {
  const accounts = new Map<string, string>();
  let nextToken: string | undefined;

  do {
    const response = await orgClient.send(
      new ListAccountsCommand({ NextToken: nextToken })
    );

    if (response.Accounts) {
      for (const account of response.Accounts) {
        if (account.Id && account.Name) {
          accounts.set(account.Id, account.Name);
        }
      }
    }

    nextToken = response.NextToken;
  } while (nextToken);

  return accounts;
}

async function getIdentityStoreId(
  ssoAdminClient: SSOAdminClient
): Promise<{ instanceArn: string; identityStoreId: string }> {
  const response = await ssoAdminClient.send(new ListInstancesCommand({}));

  if (!response.Instances || response.Instances.length === 0) {
    throw new Error("No SSO instances found");
  }

  const instance = response.Instances[0];
  if (!instance.InstanceArn || !instance.IdentityStoreId) {
    throw new Error("Instance ARN or Identity Store ID not found");
  }

  return {
    instanceArn: instance.InstanceArn,
    identityStoreId: instance.IdentityStoreId,
  };
}

async function getAllPermissionSets(
  ssoAdminClient: SSOAdminClient,
  instanceArn: string
): Promise<Map<string, string>> {
  const permissionSets = new Map<string, string>();
  let nextToken: string | undefined;

  do {
    const response = await ssoAdminClient.send(
      new ListPermissionSetsCommand({
        InstanceArn: instanceArn,
        NextToken: nextToken,
      })
    );

    if (response.PermissionSets) {
      for (const permissionSetArn of response.PermissionSets) {
        const details = await ssoAdminClient.send(
          new DescribePermissionSetCommand({
            InstanceArn: instanceArn,
            PermissionSetArn: permissionSetArn,
          })
        );

        const name = details.PermissionSet?.Name || "";
        permissionSets.set(permissionSetArn, name);
      }
    }

    nextToken = response.NextToken;
  } while (nextToken);

  return permissionSets;
}

async function getUserName(
  identityStoreClient: IdentitystoreClient,
  identityStoreId: string,
  userId: string
): Promise<string> {
  // Check cache first
  if (userCache.has(userId)) {
    return userCache.get(userId)!;
  }

  try {
    const response = await retryWithBackoff(
      () =>
        identityStoreClient.send(
          new DescribeUserCommand({
            IdentityStoreId: identityStoreId,
            UserId: userId,
          })
        ),
      `DescribeUser(${userId.substring(0, 8)}...)`
    );

    const userName = response.UserName || userId;
    userCache.set(userId, userName);
    return userName;
  } catch (error: any) {
    // Silently handle deleted users
    const result = error.name === 'ResourceNotFoundException'
      ? `[Deleted User: ${userId}]`
      : userId;

    if (error.name !== 'ResourceNotFoundException') {
      console.error(`Error fetching user ${userId}:`, error);
    }

    userCache.set(userId, result);
    return result;
  }
}

async function getGroupName(
  identityStoreClient: IdentitystoreClient,
  identityStoreId: string,
  groupId: string
): Promise<string> {
  // Check cache first
  if (groupCache.has(groupId)) {
    return groupCache.get(groupId)!;
  }

  try {
    const response = await retryWithBackoff(
      () =>
        identityStoreClient.send(
          new DescribeGroupCommand({
            IdentityStoreId: identityStoreId,
            GroupId: groupId,
          })
        ),
      `DescribeGroup(${groupId.substring(0, 8)}...)`
    );

    const groupName = response.DisplayName || groupId;
    groupCache.set(groupId, groupName);
    return groupName;
  } catch (error: any) {
    // Silently handle deleted groups
    const result = error.name === 'ResourceNotFoundException'
      ? `[Deleted Group: ${groupId}]`
      : groupId;

    if (error.name !== 'ResourceNotFoundException') {
      console.error(`Error fetching group ${groupId}:`, error);
    }

    groupCache.set(groupId, result);
    return result;
  }
}

async function getGroupMembers(
  identityStoreClient: IdentitystoreClient,
  identityStoreId: string,
  groupId: string
): Promise<string[]> {
  // Check cache first
  if (groupMembersCache.has(groupId)) {
    return groupMembersCache.get(groupId)!;
  }

  const members: string[] = [];
  let nextToken: string | undefined;

  try {
    do {
      const response = await identityStoreClient.send(
        new ListGroupMembershipsCommand({
          IdentityStoreId: identityStoreId,
          GroupId: groupId,
          NextToken: nextToken,
        })
      );

      if (response.GroupMemberships) {
        for (const membership of response.GroupMemberships) {
          if (membership.MemberId?.UserId) {
            members.push(membership.MemberId.UserId);
          }
        }
      }

      nextToken = response.NextToken;
    } while (nextToken);
  } catch (error: any) {
    // Silently handle deleted groups
    if (error.name !== 'ResourceNotFoundException') {
      console.error(`Error fetching group members for ${groupId}:`, error);
    }
  }

  groupMembersCache.set(groupId, members);
  return members;
}

async function getAssignments(
  ssoAdminClient: SSOAdminClient,
  identityStoreClient: IdentitystoreClient,
  instanceArn: string,
  identityStoreId: string,
  accountId: string,
  permissionSetArn: string,
  permissionSetName: string
): Promise<AdminAssignment[]> {
  const assignments: AdminAssignment[] = [];
  let nextToken: string | undefined;

  do {
    const response = await retryWithBackoff(
      () =>
        ssoAdminClient.send(
          new ListAccountAssignmentsCommand({
            InstanceArn: instanceArn,
            AccountId: accountId,
            PermissionSetArn: permissionSetArn,
            NextToken: nextToken,
          })
        ),
      `ListAccountAssignments(${accountId})`
    );

    if (response.AccountAssignments) {
      for (const assignment of response.AccountAssignments) {
        if (!assignment.PrincipalId || !assignment.PrincipalType) continue;

        if (assignment.PrincipalType === "USER") {
          const username = await getUserName(
            identityStoreClient,
            identityStoreId,
            assignment.PrincipalId
          );

          assignments.push({
            accountNumber: accountId,
            accountName: "",
            username,
            permissionSetName,
            assignmentType: "USER",
          });
        } else if (assignment.PrincipalType === "GROUP") {
          const groupName = await getGroupName(
            identityStoreClient,
            identityStoreId,
            assignment.PrincipalId
          );

          const members = await getGroupMembers(
            identityStoreClient,
            identityStoreId,
            assignment.PrincipalId
          );

          for (const memberId of members) {
            const username = await getUserName(
              identityStoreClient,
              identityStoreId,
              memberId
            );

            assignments.push({
              accountNumber: accountId,
              accountName: "",
              username,
              permissionSetName,
              assignmentType: "GROUP",
              groupName,
            });
          }
        }
      }
    }

    nextToken = response.NextToken;
  } while (nextToken);

  return assignments;
}

async function main() {
  try {
    console.log(`Using AWS region: ${AWS_REGION}`);
    console.log(`Concurrency limit: ${CONCURRENCY_LIMIT} parallel requests`);
    console.log(`Max retries: ${MAX_RETRIES}\n`);

    // Initialize clients
    // Note: AWS Organizations is a global service, but still requires a region
    const orgClient = new OrganizationsClient({ region: AWS_REGION });
    const ssoAdminClient = new SSOAdminClient({ region: AWS_REGION });
    const identityStoreClient = new IdentitystoreClient({ region: AWS_REGION });

    console.log("Fetching organization accounts...");
    const accounts = await getOrgAccounts(orgClient);
    console.log(`Found ${accounts.size} accounts\n`);

    console.log("Fetching SSO instance...");
    const { instanceArn, identityStoreId } = await getIdentityStoreId(
      ssoAdminClient
    );
    console.log(`Instance ARN: ${instanceArn}`);
    console.log(`Identity Store ID: ${identityStoreId}\n`);

    console.log("Fetching all permission sets...");
    const allPermissionSets = await getAllPermissionSets(
      ssoAdminClient,
      instanceArn
    );
    console.log(`Found ${allPermissionSets.size} permission sets:`);
    for (const [, name] of allPermissionSets) {
      console.log(`  - ${name}`);
    }
    console.log();

    console.log("Fetching all assignments...\n");
    const allAssignments: AdminAssignment[] = [];

    const totalAccounts = accounts.size;
    const totalOperations = totalAccounts * allPermissionSets.size;
    let completedOperations = 0;

    // Get terminal dimensions
    const terminalHeight = process.stdout.rows || 24;
    const terminalWidth = process.stdout.columns || 80;

    // Calculate available space for logs (subtract space for progress bars, headers, separators)
    const FIXED_LINES = 8; // Progress section + headers + separators
    const MAX_LOG_LINES = Math.max(6, terminalHeight - FIXED_LINES);

    // Log buffer for activity
    const logMessages: string[] = [];

    const addLog = (message: string, type: "info" | "success" | "warning" = "info") => {
      const timestamp = new Date().toLocaleTimeString();
      let icon = "•";
      let colorFn = chalk.gray;

      if (type === "success") {
        icon = "✓";
        colorFn = chalk.green;
      } else if (type === "warning") {
        icon = "⚠";
        colorFn = chalk.yellow;
      }

      logMessages.push(colorFn(`[${timestamp}] ${icon} ${message}`));
      if (logMessages.length > MAX_LOG_LINES) {
        logMessages.shift();
      }
    };

    // Set global logger for retry function
    globalLogger = addLog;

    const renderUI = () => {
      const overallPercent = Math.round((completedOperations / totalOperations) * 100);
      const accountsProcessed = Math.floor(completedOperations / allPermissionSets.size);
      const accountPercent = Math.round((accountsProcessed / totalAccounts) * 100);

      const barLength = 40;
      const overallFilled = Math.round((overallPercent / 100) * barLength);
      const accountFilled = Math.round((accountPercent / 100) * barLength);

      const overallBar =
        chalk.cyan("█").repeat(overallFilled) +
        chalk.gray("░").repeat(barLength - overallFilled);

      const accountBar =
        chalk.green("█").repeat(accountFilled) +
        chalk.gray("░").repeat(barLength - accountFilled);

      // Create separator that fits terminal width
      const separator = chalk.bold.gray("─".repeat(Math.min(terminalWidth, 100)));

      // Format percentages to be right-aligned (3 chars)
      const overallPercentStr = `${overallPercent}`.padStart(3);
      const accountPercentStr = `${accountPercent}`.padStart(3);

      // Pad log area to fill available space
      const logLines = logMessages.map(msg => `  ${msg}`);
      const emptyLines = Math.max(0, MAX_LOG_LINES - logLines.length);
      for (let i = 0; i < emptyLines; i++) {
        logLines.push("");
      }

      const output = [
        "",
        chalk.bold("Progress:"),
        `${chalk.cyan("Overall ")} │ ${overallBar} │ ${overallPercentStr}% │ ${completedOperations}/${totalOperations}`,
        `${chalk.green("Accounts")} │ ${accountBar} │ ${accountPercentStr}% │ ${accountsProcessed}/${totalAccounts}`,
        "",
        separator,
        chalk.bold("Recent Activity:"),
        ...logLines,
        separator,
        "",
      ].join("\n");

      logUpdate(output);
    };

    addLog("Starting to process accounts...");

    // Limit concurrent API calls to avoid rate limiting
    const limit = pLimit(CONCURRENCY_LIMIT);
    let processedAccounts = 0;

    // Create all tasks upfront (account + permission set combinations)
    const allTasks: Array<{
      accountId: string;
      accountName: string;
      permissionSetArn: string;
      permissionSetName: string;
    }> = [];

    for (const [accountId, accountName] of accounts) {
      for (const [permissionSetArn, permissionSetName] of allPermissionSets) {
        allTasks.push({
          accountId,
          accountName,
          permissionSetArn,
          permissionSetName,
        });
      }
    }

    // Track which accounts we've logged
    const loggedAccounts = new Set<string>();

    // Process all tasks with controlled concurrency
    const taskPromises = allTasks.map((task) =>
      limit(async () => {
        const shortAccountName =
          task.accountName.length > 50
            ? task.accountName.substring(0, 47) + "..."
            : task.accountName;

        const shortPermSetName =
          task.permissionSetName.length > 30
            ? task.permissionSetName.substring(0, 27) + "..."
            : task.permissionSetName;

        const assignments = await getAssignments(
          ssoAdminClient,
          identityStoreClient,
          instanceArn,
          identityStoreId,
          task.accountId,
          task.permissionSetArn,
          task.permissionSetName
        );

        // Add account name to assignments
        for (const assignment of assignments) {
          assignment.accountName = task.accountName;
        }

        completedOperations++;

        // Log when we find assignments
        if (assignments.length > 0) {
          addLog(
            `${shortAccountName} / ${shortPermSetName}: ${assignments.length} assignment${
              assignments.length > 1 ? "s" : ""
            }`,
            "info"
          );
        }

        // Update account progress (count unique accounts processed)
        const accountsProcessed = Math.floor(
          completedOperations / allPermissionSets.size
        );
        if (accountsProcessed > processedAccounts) {
          processedAccounts = accountsProcessed;

          // Log when account completes
          if (!loggedAccounts.has(task.accountId)) {
            loggedAccounts.add(task.accountId);
            addLog(`Completed: ${shortAccountName}`, "success");
          }
        }

        renderUI();

        return { task, assignments };
      })
    );

    const allTaskResults = await Promise.all(taskPromises);

    // Group results by account
    const accountMap = new Map<string, AdminAssignment[]>();
    for (const { task, assignments } of allTaskResults) {
      if (!accountMap.has(task.accountId)) {
        accountMap.set(task.accountId, []);
      }
      accountMap.get(task.accountId)!.push(...assignments);
    }

    // Flatten to single array
    for (const assignments of accountMap.values()) {
      allAssignments.push(...assignments);
    }

    // Final update
    addLog(`Processing complete! ${allAssignments.length} total assignments found`, "success");
    renderUI();
    logUpdate.done();

    console.log(chalk.green(`\n✓ Complete! Found ${allAssignments.length} total assignments\n`));

    // Write results to CSV file
    const csvLines = ["Account Number,Account Name,Username,Permission Set,Group"];

    for (const assignment of allAssignments) {
      // Escape values that contain commas or quotes
      const escapeCsv = (value: string) => {
        if (value.includes(',') || value.includes('"') || value.includes('\n')) {
          return `"${value.replace(/"/g, '""')}"`;
        }
        return value;
      };

      csvLines.push(
        [
          escapeCsv(assignment.accountNumber),
          escapeCsv(assignment.accountName),
          escapeCsv(assignment.username),
          escapeCsv(assignment.permissionSetName),
          escapeCsv(assignment.groupName || '')
        ].join(',')
      );
    }

    const csvContent = csvLines.join('\n');
    const outputPath = path.resolve(OUTPUT_FILE);

    fs.writeFileSync(outputPath, csvContent, 'utf-8');

    console.log(chalk.cyan(`📄 Results written to: ${outputPath}`));
    console.log(chalk.cyan(`📊 Total assignments: ${allAssignments.length}`));
  } catch (error) {
    console.error("Error:", error);
    process.exit(1);
  }
}

main();
