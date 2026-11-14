param(
    [int]$Runs = 3,
    [int]$SizeMiB = 256,
    [int]$Connections = 4,
    [int]$PieceMiB = 8
)

$ErrorActionPreference = 'Stop'

function New-Fixture($Path, [int64]$Bytes) {
    $buffer = New-Object byte[] (1024 * 1024)
    for ($i = 0; $i -lt $buffer.Length; $i++) {
        $buffer[$i] = [byte]($i % 251)
    }

    $stream = [System.IO.File]::Create($Path)
    try {
        $remaining = $Bytes
        while ($remaining -gt 0) {
            $write = [Math]::Min($buffer.Length, $remaining)
            $stream.Write($buffer, 0, $write)
            $remaining -= $write
        }
    } finally {
        $stream.Dispose()
    }
}

function New-RangeServer($Root) {
    $script = Join-Path $Root 'range_server.py'
    @'
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import os
import sys

root = sys.argv[1]
port = int(sys.argv[2])
path = os.path.join(root, "fixture.bin")
size = os.path.getsize(path)
with open(path, "rb") as f:
    etag = '"' + hashlib.sha256(f.read()).hexdigest() + '"'

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(size))
        self.send_header("ETag", etag)
        self.send_header("Connection", "close")
        self.end_headers()

    def do_GET(self):
        if self.path != "/fixture.bin":
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            return

        start = 0
        end = size - 1
        status = 200
        range_header = self.headers.get("Range")
        if range_header:
            unit, _, spec = range_header.partition("=")
            left, _, right = spec.partition("-")
            start = int(left)
            end = int(right) if right else size - 1
            status = 206

        length = end - start + 1
        self.send_response(status)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(length))
        self.send_header("ETag", etag)
        self.send_header("Connection", "close")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()

        with open(path, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining:
                chunk = f.read(min(1024 * 1024, remaining))
                if not chunk:
                    break
                self.wfile.write(chunk)
                remaining -= len(chunk)

ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
'@ | Set-Content -LiteralPath $script -Encoding UTF8
    return $script
}

function Get-FreePort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    $port = $listener.LocalEndpoint.Port
    $listener.Stop()
    return $port
}

function Wait-Port($Port) {
    for ($i = 0; $i -lt 100; $i++) {
        try {
            $client = [System.Net.Sockets.TcpClient]::new()
            $client.Connect('127.0.0.1', $Port)
            $client.Close()
            return
        } catch {
            Start-Sleep -Milliseconds 100
        }
    }
    throw "server did not start"
}

function Invoke-Timed($Name, $Command, $Arguments, $Output, $ExpectedHash) {
    Remove-Item -LiteralPath $Output -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath "$Output.part" -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath "$Output.part.meta" -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath "$Output.part.ranges" -Force -ErrorAction SilentlyContinue

    $started = [System.Diagnostics.Stopwatch]::StartNew()
    & $Command @Arguments *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "$Name failed with exit code $LASTEXITCODE"
    }
    $started.Stop()

    $actualHash = (Get-FileHash -Algorithm SHA256 $Output).Hash
    if ($actualHash -ne $ExpectedHash) {
        throw "$Name hash mismatch: $actualHash != $ExpectedHash"
    }

    [pscustomobject]@{
        tool = $Name
        seconds = [Math]::Round($started.Elapsed.TotalSeconds, 3)
        mib_per_sec = [Math]::Round($SizeMiB / $started.Elapsed.TotalSeconds, 2)
    }
}

$repo = Resolve-Path (Join-Path $PSScriptRoot '..')
$chorus = Join-Path $repo 'target\release\Chorus.exe'
$aria2 = (Get-Command aria2c -ErrorAction Stop).Source
$root = Join-Path $env:TEMP ("chorus-bench-{0}" -f $PID)
$culture = [System.Globalization.CultureInfo]::InvariantCulture
New-Item -ItemType Directory -Path $root -Force | Out-Null

try {
    cargo build --release --quiet
    New-Fixture (Join-Path $root 'fixture.bin') ([int64]$SizeMiB * 1024 * 1024)
    $expectedHash = (Get-FileHash -Algorithm SHA256 (Join-Path $root 'fixture.bin')).Hash

    $serverScript = New-RangeServer $root
    $port = Get-FreePort
    $server = Start-Process python -ArgumentList @($serverScript, $root, $port) -PassThru -WindowStyle Hidden
    Wait-Port $port

    $url = "http://127.0.0.1:$port/fixture.bin"
    "tool,run,seconds,mib_per_sec"
    for ($run = 1; $run -le $Runs; $run++) {
        $chorusOut = Join-Path $root "chorus-$run.bin"
        $ariaOut = Join-Path $root "aria2-$run.bin"

        $result = Invoke-Timed 'chorus' $chorus @(
            '-x', "$Connections",
            '--piece-size', "$([int64]$PieceMiB * 1024 * 1024)",
            '--sha256', $expectedHash.ToLowerInvariant(),
            '--no-resume',
            '-o', $chorusOut,
            $url
        ) $chorusOut $expectedHash
        [string]::Format($culture, "{0},{1},{2:0.000},{3:0.00}", $result.tool, $run, $result.seconds, $result.mib_per_sec)

        $result = Invoke-Timed 'aria2c' $aria2 @(
            '--allow-overwrite=true',
            '--auto-file-renaming=false',
            '--check-certificate=false',
            '--console-log-level=error',
            '--download-result=hide',
            '--file-allocation=none',
            '--max-connection-per-server', "$Connections",
            '--min-split-size', "${PieceMiB}M",
            '--split', "$Connections",
            '--dir', $root,
            '--out', (Split-Path -Leaf $ariaOut),
            $url
        ) $ariaOut $expectedHash
        [string]::Format($culture, "{0},{1},{2:0.000},{3:0.00}", $result.tool, $run, $result.seconds, $result.mib_per_sec)
    }
} finally {
    if ($server -and -not $server.HasExited) {
        Stop-Process -Id $server.Id -Force
    }
    Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue
}
