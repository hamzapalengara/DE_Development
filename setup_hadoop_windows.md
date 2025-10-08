# Setup Hadoop for PySpark on Windows

## The Issue
PySpark on Windows requires Hadoop's `winutils.exe` to write files locally.

## Quick Fix Steps

### Step 1: Download winutils.exe
1. Download winutils.exe from: https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe
2. Create a directory: `C:\hadoop\bin`
3. Place `winutils.exe` in `C:\hadoop\bin\`

### Step 2: Set Environment Variable
Run this in PowerShell (as Administrator):
```powershell
[System.Environment]::SetEnvironmentVariable('HADOOP_HOME', 'C:\hadoop', [System.EnvironmentVariableTarget]::Machine)
```

Or manually:
1. Open System Properties → Environment Variables
2. Add new System Variable:
   - Variable name: `HADOOP_HOME`
   - Variable value: `C:\hadoop`

### Step 3: Restart Your Terminal
Close and reopen your PowerShell/terminal for the changes to take effect.

### Step 4: Verify
```powershell
echo $env:HADOOP_HOME
# Should output: C:\hadoop
```

## Alternative: Use WSL (Windows Subsystem for Linux)
If you prefer, you can run PySpark in WSL where Hadoop dependencies work natively without winutils.exe.
