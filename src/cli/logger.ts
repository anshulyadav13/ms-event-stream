/**
 * Logger utility for the CLI tool
 * Handles different log levels and output formats
 */
export class Logger {
  private verbose = false;
  private jsonOutput = false;

  /**
   * Set verbose logging mode
   */
  setVerbose(enabled: boolean): void {
    this.verbose = enabled;
  }

  /**
   * Set JSON output mode
   */
  setJsonOutput(enabled: boolean): void {
    this.jsonOutput = enabled;
  }

  /**
   * Log an info message
   */
  info(message: string, data?: any): void {
    this.log('info', message, data);
  }

  /**
   * Log a warning message
   */
  warn(message: string, data?: any): void {
    this.log('warn', message, data);
  }

  /**
   * Log an error message
   */
  error(message: string, data?: any): void {
    this.log('error', message, data);
  }

  /**
   * Log a debug message (only in verbose mode)
   */
  debug(message: string, data?: any): void {
    if (this.verbose) {
      this.log('debug', message, data);
    }
  }

  /**
   * Log a success message
   */
  success(message: string, data?: any): void {
    this.log('success', message, data);
  }

  /**
   * Internal logging method
   */
  private log(level: string, message: string, data?: any): void {
    if (this.jsonOutput) {
      this.logJson(level, message, data);
    } else {
      this.logText(level, message, data);
    }
  }

  /**
   * Log in JSON format
   */
  private logJson(level: string, message: string, data?: any): void {
    const logEntry = {
      timestamp: new Date().toISOString(),
      level,
      message,
      ...(data && { data })
    };

    console.log(JSON.stringify(logEntry));
  }

  /**
   * Log in human-readable text format
   */
  private logText(level: string, message: string, data?: any): void {
    const timestamp = new Date().toISOString();
    const levelEmoji = this.getLevelEmoji(level);
    const levelColor = this.getLevelColor(level);
    
    // Format timestamp
    const timeStr = timestamp.split('T')[1].split('.')[0];
    
    // Format level
    const levelStr = level.toUpperCase().padEnd(5);
    
    // Build log line
    let logLine = `${levelEmoji} ${timeStr} [${levelStr}] ${message}`;
    
    // Add data if present
    if (data) {
      if (typeof data === 'object') {
        logLine += `\n${JSON.stringify(data, null, 2)}`;
      } else {
        logLine += ` ${data}`;
      }
    }

    // Output to console
    if (level === 'error') {
      console.error(logLine);
    } else if (level === 'warn') {
      console.warn(logLine);
    } else {
      console.log(logLine);
    }
  }

  /**
   * Get emoji for log level
   */
  private getLevelEmoji(level: string): string {
    switch (level) {
      case 'info': return 'ℹ️';
      case 'warn': return '⚠️';
      case 'error': return '❌';
      case 'debug': return '🔍';
      case 'success': return '✅';
      default: return '📝';
    }
  }

  /**
   * Get color for log level (for future terminal color support)
   */
  private getLevelColor(level: string): string {
    switch (level) {
      case 'info': return 'blue';
      case 'warn': return 'yellow';
      case 'error': return 'red';
      case 'debug': return 'gray';
      case 'success': return 'green';
      default: return 'white';
    }
  }

  /**
   * Print a table of data
   */
  table(data: any[]): void {
    if (this.jsonOutput) {
      console.log(JSON.stringify(data, null, 2));
    } else {
      console.table(data);
    }
  }

  /**
   * Print a progress bar
   */
  progress(current: number, total: number, label = 'Progress'): void {
    if (this.jsonOutput) {
      return; // Skip progress bars in JSON mode
    }

    const percentage = Math.round((current / total) * 100);
    const barLength = 30;
    const filledLength = Math.round((barLength * current) / total);
    
    const bar = '█'.repeat(filledLength) + '░'.repeat(barLength - filledLength);
    
    process.stdout.write(`\r${label}: [${bar}] ${percentage}% (${current}/${total})`);
    
    if (current === total) {
      process.stdout.write('\n');
    }
  }

  /**
   * Clear the current line
   */
  clearLine(): void {
    if (!this.jsonOutput) {
      process.stdout.write('\r\x1b[K');
    }
  }

  /**
   * Print a separator line
   */
  separator(char = '─', length = 80): void {
    if (!this.jsonOutput) {
      console.log(char.repeat(length));
    }
  }

  /**
   * Print a header
   */
  header(title: string): void {
    if (!this.jsonOutput) {
      this.separator('═');
      console.log(`  ${title}`);
      this.separator('═');
    }
  }
}

