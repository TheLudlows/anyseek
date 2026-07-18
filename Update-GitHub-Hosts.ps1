param(
    [string]$GitHubUrl = "https://raw.githubusercontent.com/521xiao/hosts/master/hosts",
    [string]$HostsPath = "$env:SystemRoot\System32\drivers\etc\hosts",
    [string]$BackupPath = "$env:SystemRoot\System32\drivers\etc\hosts.bak",
    [switch]$Force
)

# 检查管理员权限
if (-not ([Security.Principal.WindowsPrincipal]::new().IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator))) {
    Write-Error "此脚本需要管理员权限运行" -CategoryPermissionDenied
    exit 1
}

# 检查备份
if (Test-Path $HostsPath -PathType Leaf) {
    if (-not (Test-Path $BackupPath -PathType Leaf) -and $Force) {
        Write-Host "创建备份: $BackupPath" -ForegroundColor Cyan
        Copy-Item $HostsPath $BackupPath -Force
    }
}

# 尝试从 GitHub 获取
Write-Host "正在从 GitHub 获取最新的 hosts 文件..." -ForegroundColor Cyan
try {
    $content = Invoke-WebRequest -Uri $GitHubUrl -TimeoutSec 30 -ErrorAction Stop
    $hostsContent = $content.Content
} catch {
    Write-Host "从 GitHub 获取失败: $_" -ForegroundColor Red
    Write-Host "尝试备用源..." -ForegroundColor Yellow
    try {
        # Gitee 镜像
        $backup_url = $GitHubUrl.Replace("github.com", "gitee.com")
        Write-Host "尝试从 Gitee 获取..."
        $content = Invoke-WebRequest -Uri $backup_url -TimeoutSec 30 -ErrorAction Stop
        $hostsContent = $content.Content
    } catch {
        Write-Host "Gitee 也无法访问，请手动获取 hosts 文件" -ForegroundColor Red
        Write-Host "下载地址: $GitHubUrl" -ForegroundColor Cyan
        exit 1
    }
}

# 检查内容是否有效
if (-not $hostsContent) {
    Write-Error "获取到的内容为空" -Category ObjectNotFound
    exit 1
}

# 创建临时文件
$tempPath = "$env:TEMP\hosts.tmp"
$hostsContent | Out-File -FilePath $tempPath -Encoding UTF8

# 备份并更新
Write-Host "正在更新 hosts 文件..." -ForegroundColor Cyan
try {
    Copy-Item $HostsPath $BackupPath -Force -ErrorAction Stop
    Move-Item $tempPath $HostsPath -Force -ErrorAction Stop
    Write-Host "✅ hosts 文件已更新" -ForegroundColor Green

    # 验证更新
    $updated = Get-Content $HostsPath -Raw -Encoding UTF8
    if ($updated -match "github" -or $updated -match "github.global" -or $updated -match "140.82") {
        Write-Host "✅ 验证成功：发现 GitHub 相关条目" -ForegroundColor Green
    } else {
        Write-Host "⚠️  警告：未发现 GitHub 相关条目" -ForegroundColor Yellow
    }
} catch {
    Write-Host "更新失败: $_" -ForegroundColor Red
    # 恢复备份
    if (Test-Path $BackupPath) {
        Copy-Item $BackupPath $HostsPath -Force
        Write-Host "已恢复备份文件" -ForegroundColor Yellow
    }
    Remove-Item $tempPath -Force -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "脚本执行完成！" -ForegroundColor Green
Write-Host "提示：如果更新后 GitHub 访问仍然缓慢，请尝试" -ForegroundColor Cyan
Write-Host "  1. 使用代理工具（Clash、V2Ray）" -ForegroundColor Cyan
Write-Host "  2. 启用 GitHub Desktop 的 DNS 加速功能" -ForegroundColor Cyan
Write-Host "   3. 使用国内镜像源（如 Gitee、Coding.net）" -ForegroundColor Cyan