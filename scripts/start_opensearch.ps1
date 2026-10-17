<#
.SYNOPSIS
Start the project's local OpenSearch instance for retrieval comparisons.
.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\scripts\start_opensearch.ps1
.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\scripts\start_opensearch.ps1 -Port 9201 -HeapSize 2g
.NOTES
Official distribution: https://artifacts.opensearch.org/releases/bundle/opensearch/3.9.0/opensearch-3.9.0-windows-x64.zip
Requires the archive extracted into tools/opensearch/opensearch-3.9.0.
HTTP endpoint: http://127.0.0.1:9200 (default). Stop with Ctrl+C.
Security is disabled for local testing; binds only to the loopback interface.
Data and logs stay in the ignored distribution directory.
#>
[CmdletBinding()]
param(
    [ValidateRange(1024, 65535)]
    [int]$Port = 9200,
    [ValidatePattern('^[1-9][0-9]*[mgMG]$')]
    [string]$HeapSize = '1g'
)

$ErrorActionPreference = 'Stop'
$distribution = Join-Path (Split-Path -Parent $PSScriptRoot) 'tools\opensearch\opensearch-3.9.0'
$launcher = Join-Path $distribution 'bin\opensearch.bat'
if (-not (Test-Path -LiteralPath $launcher)) {
    throw "OpenSearch is missing. Extract opensearch-3.9.0-windows-x64.zip into tools/opensearch first."
}

$previousJava = $env:OPENSEARCH_JAVA_HOME
$previousOptions = $env:OPENSEARCH_JAVA_OPTS
try {
    $env:OPENSEARCH_JAVA_HOME = Join-Path $distribution 'jdk'
    $env:OPENSEARCH_JAVA_OPTS = "-Xms$HeapSize -Xmx$HeapSize"
    Push-Location $distribution
    try {
        & $launcher '-Ecluster.name=seekpower-comparison' '-Enode.name=local-comparison' `
            '-Enetwork.host=127.0.0.1' '-Ediscovery.type=single-node' `
            '-Eplugins.security.disabled=true' "-Ehttp.port=$Port"
        if ($LASTEXITCODE -ne 0) {
            throw "OpenSearch exited with code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
} finally {
    $env:OPENSEARCH_JAVA_HOME = $previousJava
    $env:OPENSEARCH_JAVA_OPTS = $previousOptions
}
