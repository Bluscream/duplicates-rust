#requires -Version 7.0
<#
.SYNOPSIS
    Finds and handles duplicate files. Features CSV hashing cache for performance and resume support.
#>
param(
    [Parameter(Position = 0)]
    [Alias('p')]
    [string]$Path = ".",

    [Alias('r')]
    [switch]$Recursive,

    [Alias('d')]
    [switch]$DryRun,

    [Parameter(Mandatory = $true)]
    [ValidateSet('latest', 'oldest', 'highest', 'deepest', 'first', 'last')]
    [Alias('k')]
    [string]$Keep,

    [Alias('m')]
    [ValidateSet('lnk', 'symlink', 'hardlink', 'delete')]
    [string]$Mode = 'symlink',

    [Alias('a')]
    [ValidateSet('name', 'size', 'crc32', 'md5', 'sha256', 'sha512')]
    [string]$Algorithm = 'md5',

    [Alias('i')]
    [string[]]$Ignore = @('symlink', '.lnk', '.url')
)

#region Global Setup & Logging
$targetPath = Convert-Path $Path
$logFile = Join-Path $targetPath "duplicates.log"
$hashCsv = Join-Path $targetPath "duplicates.hashes.csv"
$Ignore += "duplicates.log", "duplicates.hashes.csv"

function Log-Output {
    param([string]$Message, [string]$Color = "White")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $formattedMsg = "[$timestamp] $Message"
    Write-Host $Message -ForegroundColor $Color
    $formattedMsg | Add-Content -Path $logFile
}

function Get-DiskSpaceRaw {
    $drive = Get-CimInstance Win32_LogicalDisk -Filter "DeviceID='$($targetPath.Substring(0,2))'"
    return @{ Total = $drive.Size; Free = $drive.FreeSpace }
}

function Format-DiskInfo {
    param($Free, $Total)
    $totalGB = [math]::Round($Total / 1GB, 2)
    $freeGB = [math]::Round($Free / 1GB, 2)
    $percent = if ($Total -gt 0) { [math]::Round(($Free / $Total) * 100, 1) } else { 0 }
    return "$freeGB/${totalGB}GB ($percent%)"
}

# Clear old log if starting fresh
$null = New-Item -Path $logFile -ItemType File -Force
Log-Output "Settings: Path=$targetPath | Keep=$Keep | Mode=$Mode | Algorithm=$Algorithm | Recursive=$Recursive | DryRun=$DryRun | Ignore=$($Ignore -join ',')" "Yellow"
$initialDisk = Get-DiskSpaceRaw
Log-Output "Free space before: $(Format-DiskInfo -Free $initialDisk.Free -Total $initialDisk.Total)" "Yellow"
#endregion

#region Native Helper Methods
Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using System.IO;

public class FileUtil {
    [StructLayout(LayoutKind.Sequential)]
    public struct BY_HANDLE_FILE_INFORMATION {
        public uint FileAttributes;
        public System.Runtime.InteropServices.ComTypes.FILETIME CreationTime;
        public System.Runtime.InteropServices.ComTypes.FILETIME LastAccessTime;
        public System.Runtime.InteropServices.ComTypes.FILETIME LastWriteTime;
        public uint VolumeSerialNumber;
        public uint FileSizeHigh;
        public uint FileSizeLow;
        public uint nNumberOfLinks;
        public uint nFileIndexHigh;
        public uint nFileIndexLow;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool GetFileInformationByHandle(SafeFileHandle hFile, out BY_HANDLE_FILE_INFORMATION lpFileInformation);

    public static string GetFileId(string path) {
        try {
            using (var fileHandle = System.IO.File.OpenHandle(path, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite)) {
                BY_HANDLE_FILE_INFORMATION info;
                if (GetFileInformationByHandle(fileHandle, out info)) {
                    return info.VolumeSerialNumber.ToString("X") + ":" + info.nFileIndexHigh.ToString("X") + ":" + info.nFileIndexLow.ToString("X");
                }
            }
        } catch {}
        return null;
    }

    private static uint[] crcTable;
    static FileUtil() {
        uint polynomial = 0xedb88320;
        crcTable = new uint[256];
        for (uint i = 0; i < 256; i++) {
            uint crc = i;
            for (int j = 8; j > 0; j--) {
                if ((crc & 1) == 1) crc = (crc >> 1) ^ polynomial;
                else crc >>= 1;
            }
            crcTable[i] = crc;
        }
    }

    public static string GetCrc32(string path) {
        try {
            uint crc = 0xffffffff;
            byte[] buffer = new byte[4096];
            using (FileStream fs = File.OpenRead(path)) {
                int count;
                while ((count = fs.Read(buffer, 0, buffer.Length)) > 0) {
                    for (int i = 0; i < count; i++) {
                        crc = (crc >> 8) ^ crcTable[(crc ^ buffer[i]) & 0xff];
                    }
                }
            }
            return (~crc).ToString("X8");
        } catch { return null; }
    }
}
"@
#endregion

#region Cache Management
$hashCache = @{}
if (Test-Path $hashCsv) {
    Log-Output "Loading existing hash cache from $hashCsv..." "Cyan"
    $lines = Get-Content $hashCsv
    $corruptLines = 0
    foreach ($line in $lines) {
        if ($line -match '^path;size;time;type;hash$' -or [string]::IsNullOrWhiteSpace($line)) { continue }
        $parts = $line -split ';'
        $isValid = $false
        
        if ($parts.Count -eq 5) {
            $cPath = $parts[0]; $cSize = $parts[1]; $cTime = $parts[2]; $cType = $parts[3]; $cHash = $parts[4]
            $fullPath = Join-Path $targetPath $cPath
            
            # Validation Logic
            if ((Test-Path $fullPath) -and 
                ($cSize -match '^\d+$') -and 
                ($cTime -match '^\d+$') -and 
                ('name', 'size', 'crc32', 'md5', 'sha256', 'sha512' -contains $cType)) {
                
                $hashRegex = switch ($cType) {
                    'md5' { '^[a-fA-F0-9]{32}$' }
                    'sha256' { '^[a-fA-F0-9]{64}$' }
                    'sha512' { '^[a-fA-F0-9]{128}$' }
                    'crc32' { '^[a-fA-F0-9]{8}$' }
                    default { '.*' }
                }
                
                if ($cHash -match $hashRegex) {
                    $key = "$cPath|$cSize|$cTime|$cType"
                    $hashCache[$key] = $cHash
                    $isValid = $true
                }
            }
        }
        
        if (-not $isValid) { $corruptLines++ }
    }
    if ($corruptLines -gt 0) { Log-Output "Ignored $corruptLines corrupt lines in CSV." "Red" }
} else {
    "path;size;time;type;hash" | Set-Content -Path $hashCsv -Encoding utf8
}

function Save-HashEntry {
    param($RelativePath, $Size, $Time, $Type, $Hash)
    "$RelativePath;$Size;$Time;$Type;$Hash" | Add-Content -Path $hashCsv -Encoding utf8
}
#endregion

#region Discovery & Filtering
function Get-Files {
    Log-Output "Scanning directory: $targetPath" "Cyan"
    $gciParams = @{ Path = $targetPath; File = $true; Recurse = $Recursive }
    
    $count = 0
    $initialFiles = [System.Collections.Generic.List[object]]::new()
    Get-ChildItem @gciParams | ForEach-Object {
        $count++
        if ($count % 1000 -eq 0) { Write-Host "  Files discovered: $count...`r" -NoNewline }
        
        $shouldIgnore = $false
        foreach ($p in $Ignore) {
            if ($p -eq 'symlink' -and $_.Attributes -match 'ReparsePoint') { $shouldIgnore = $true; break }
            if ($_.Name -like "*$p") { $shouldIgnore = $true; break }
        }
        if (-not $shouldIgnore) { $initialFiles.Add($_) }
    }
    Write-Host "" # Newline after progress
    return $initialFiles
}

$discoveredFiles = Get-Files
if (-not $discoveredFiles) { Log-Output "No files found." "Red"; exit }

Log-Output "Found $($discoveredFiles.Count) files. Filtering hardlinks..." "Cyan"
$uniqueFiles = @()
$seenIds = [System.Collections.Generic.HashSet[string]]::new()
$pCount = 0
foreach ($f in $discoveredFiles) {
    $pCount++
    if ($pCount % 1000 -eq 0) { Log-Output "  Processed $pCount/$($discoveredFiles.Count) files..." "Gray" }
    $id = [FileUtil]::GetFileId($f.FullName)
    if ($null -eq $id -or $seenIds.Add($id)) { $uniqueFiles += $f }
}

$allFiles = $uniqueFiles | Select-Object FullName, Name, Length, LastWriteTime, @{N = 'RelPath'; E = { $_.FullName.Substring($targetPath.Length).TrimStart('\') } }
Log-Output "After filtering: $($allFiles.Count) unique files." "Green"
#endregion

#region Grouping and Hashing
$potentialGroups = if ($Algorithm -eq 'name') {
    $allFiles | Group-Object Name | Where-Object { $_.Count -gt 1 }
} else {
    $allFiles | Group-Object Length | Where-Object { $_.Count -gt 1 }
}

if (-not $potentialGroups) { Log-Output "No duplicates found by size/name." "Green"; exit }

$hashedFiles = [System.Collections.Generic.List[object]]::new()
if ($Algorithm -match 'md5|sha|crc32') {
    $filesToHash = $potentialGroups.Group
    Log-Output "Hashing $($filesToHash.Count) files in parallel (Reuse enabled)..." "Cyan"
    
    $completed = 0
    $newHashes = $filesToHash | ForEach-Object -Parallel {
        $cache = $using:hashCache
        $algo = $using:Algorithm
        $key = "$($_.RelPath)|$($_.Length)|$($_.LastWriteTime.Ticks)|$algo"
        $found = $cache[$key]
        
        if ($found) {
            $null = $_ | Add-Member -NotePropertyName Hash -NotePropertyValue $found
            $null = $_ | Add-Member -NotePropertyName Cached -NotePropertyValue $true
        } else {
            if ($_.Length -gt 104857600) { Write-Host "Hashing $($_.RelPath) ($([math]::Round($_.Length/1MB,2)) MB)" -ForegroundColor Gray }
            $h = if ($algo -eq 'crc32') { [FileUtil]::GetCrc32($_.FullName) } else { (Get-FileHash $_.FullName -Algorithm $algo).Hash }
            if ($h) {
                $null = $_ | Add-Member -NotePropertyName Hash -NotePropertyValue $h
                $null = $_ | Add-Member -NotePropertyName Cached -NotePropertyValue $false
            }
        }
        $_
    } -ThrottleLimit 8 | ForEach-Object {
        $completed++
        if ($completed % 100 -eq 0) { Log-Output "  Hashed $completed/$($filesToHash.Count) files..." "Gray" }
        if (-not $_.Cached) {
            Save-HashEntry -RelativePath $_.RelPath -Size $_.Length -Time $_.LastWriteTime.Ticks -Type $Algorithm -Hash $_.Hash
        }
        $hashedFiles.Add($_)
    }
} else {
    $hashedFiles.AddRange($potentialGroups.Group)
}

Log-Output "Grouping by hash/criteria..." "Cyan"
$progress = 0
$finalGroups = if ($Algorithm -match 'md5|sha|crc32') {
    $hashedFiles | Group-Object Hash | Where-Object { 
        $progress++
        if ($progress % 100 -eq 0) { Log-Output "  Grouping: $progress comparisons..." "Gray" }
        $_.Count -gt 1 
    }
} else { $potentialGroups }
#endregion

#region Duplicate Processing
if (-not $finalGroups) { Log-Output "No duplicates found after internal check." "Green"; exit }

Log-Output "Found $($finalGroups.Count) groups of duplicates." "Yellow"
foreach ($g in $finalGroups) {
    $sorted = switch ($Keep) {
        'latest' { $g.Group | Sort-Object LastWriteTime -Descending }
        'oldest' { $g.Group | Sort-Object LastWriteTime }
        'highest' { $g.Group | Sort-Object { $_.FullName.Length } }
        'deepest' { $g.Group | Sort-Object { $_.FullName.Length } -Descending }
        'first' { $g.Group }
        'last' { $g.Group | Select-Object -Last $g.Group.Count }
    }
    
    $keepFile = $sorted[0]
    $dups = $sorted | Select-Object -Skip 1
    
    Log-Output "Group $($g.Name): Keeping $($keepFile.RelPath)" "Green"
    foreach ($d in $dups) {
        if ($DryRun) { Log-Output "  [DRY RUN] Would process: $($d.RelPath) -> $Mode" "Gray"; continue }
        
        try {
            if (-not (Test-Path $d.FullName)) { continue }
            Remove-Item $d.FullName -Force
            switch ($Mode) {
                'delete' { Log-Output "  Deleted $($d.RelPath)" "Red" }
                'symlink' { New-Item -ItemType SymbolicLink -Path $d.FullName -Target $keepFile.FullName -Force > $null; Log-Output "  Symlinked $($d.RelPath)" "Cyan" }
                'hardlink' { New-Item -ItemType HardLink -Path $d.FullName -Target $keepFile.FullName -Force > $null; Log-Output "  Hardlinked $($d.RelPath)" "Cyan" }
                'lnk' {
                    $s = (New-Object -ComObject WScript.Shell).CreateShortcut("$($d.FullName).lnk")
                    $s.TargetPath = $keepFile.FullName; $s.Save()
                    Log-Output "  Created .lnk for $($d.RelPath)" "Cyan"
                }
            }
        } catch { Log-Output "  ERROR processing $($d.RelPath): $_" "Red" }
    }
}
$finalDisk = Get-DiskSpaceRaw
Log-Output "Free space after: $(Format-DiskInfo -Free $finalDisk.Free -Total $finalDisk.Total)" "Yellow"
if ($null -ne $initialDisk.Free -and $null -ne $finalDisk.Free) {
    $freedBytes = $finalDisk.Free - $initialDisk.Free
    if ($freedBytes -gt 0) {
        $freedGB = [math]::Round($freedBytes / 1GB, 4)
        $freedPercent = [math]::Round(($freedBytes / $initialDisk.Total) * 100, 3)
        Log-Output "Total space freed: $freedGB GB ($freedPercent% of total disk space)" "Green"
    } else {
        Log-Output "Total space freed: 0 GB (0%)" "Green"
    }
}
#endregion
