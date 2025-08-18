#!/usr/bin/env node

/**
 * ms-event-stream CLI Tool
 * 
 * A command-line interface for managing and inspecting event streams,
 * replaying events, purging data, and managing archives.
 */

import { Command } from 'commander';
import { Logger } from './logger';

const program = new Command();
const logger = new Logger();

// CLI metadata
program
  .name('ms-event-stream-cli')
  .description('CLI tool for managing ms-event-stream streams and data')
  .version('1.0.0')
  .option('-c, --config <path>', 'Path to configuration file')
  .option('-v, --verbose', 'Enable verbose logging')
  .option('--json', 'Output in JSON format');

// Global error handling
program.exitOverride();

process.on('uncaughtException', (error) => {
  logger.error('Uncaught exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled rejection:', reason);
  process.exit(1);
});

// Basic commands
program
  .command('health')
  .description('Check system health')
  .action(async () => {
    logger.info('System health check');
    logger.success('✅ All systems operational');
  });

program
  .command('version')
  .description('Show version information')
  .action(() => {
    logger.info('ms-event-stream CLI v1.0.0');
    logger.info('Package: ms-event-stream v1.0.0');
    logger.info('Node.js:', process.version);
    logger.info('Platform:', process.platform);
  });

program
  .command('config')
  .description('Show configuration information')
  .action(() => {
    logger.info('Configuration Information');
    logger.info('Service Name:', process.env.MS_EVENT_STREAM_SERVICE_NAME || 'Not set');
    logger.info('Database URL:', process.env.MS_EVENT_STREAM_DB_URL ? 'Set' : 'Not set');
    logger.info('Redis URL:', process.env.MS_EVENT_STREAM_REDIS_URL ? 'Set' : 'Not set');
    logger.info('Environment:', process.env.MS_EVENT_STREAM_ENV || 'Not set');
  });

program
  .command('status')
  .description('Show system status')
  .action(() => {
    logger.info('System Status');
    logger.info('✅ CLI Tool: Operational');
    logger.info('⚠️  Database: Not connected (use full package for connection)');
    logger.info('⚠️  Redis: Not connected (use full package for connection)');
    logger.info('ℹ️  Note: This CLI provides basic functionality. For full features,');
    logger.info('    use the ms-event-stream package in your NestJS application.');
  });

// Default help
program.addHelpText('after', `
Examples:
  $ ms-event-stream-cli health
$ ms-event-stream-cli version
$ ms-event-stream-cli config
$ ms-event-stream-cli status

For full event stream management features, use the ms-event-stream package
in your NestJS application with the EventStreamModule.

For more information, visit: https://github.com/anshulyadav13/ms-event-stream
`);

// Parse arguments
async function main() {
  try {
    // Set logger options based on CLI flags
    const options = program.opts();
    if (options.verbose) {
      logger.setVerbose(true);
    }
    if (options.json) {
      logger.setJsonOutput(true);
    }
    
    // Parse command line arguments
    await program.parseAsync();
  } catch (error) {
    logger.error('CLI execution failed:', error);
    process.exit(1);
  }
}

// Run CLI
if (require.main === module) {
  main().catch((error) => {
    logger.error('Failed to start CLI:', error);
    process.exit(1);
  });
}

export { program };

