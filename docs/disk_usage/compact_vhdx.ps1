# Nén docker_data.vhdx: tắt Docker -> wsl --shutdown -> diskpart (UAC) -> bật lại Docker.
$sp   = Split-Path -Parent $MyInvocation.MyCommand.Path
$vhdx = 'E:\DockerDesktopWSL\disk\docker_data.vhdx'
$log  = Join-Path $sp 'compact_vhdx.log'
"before_bytes=$((Get-Item $vhdx).Length)  $(Get-Date -Format s)"

docker desktop stop 2>&1 | Out-String | Write-Output
wsl --shutdown
Start-Sleep -Seconds 8

try {
    $p = Start-Process -FilePath cmd.exe -Verb RunAs -PassThru -WindowStyle Hidden `
         -ArgumentList "/c diskpart /s `"$sp\compact_vhdx.txt`" > `"$log`" 2>&1"
    if (-not $p.WaitForExit(900000)) { "diskpart: quá 15 phút chưa xong (UAC chưa được bấm?)" }
    else { "diskpart exit_code=$($p.ExitCode)" }
} catch {
    "UAC bị từ chối hoặc lỗi: $($_.Exception.Message)"
}
if (Test-Path $log) { Get-Content $log | Where-Object { $_ -match 'percent|success|error|Lỗi|thành công|DiskPart' } }
"after_bytes=$((Get-Item $vhdx).Length)  $(Get-Date -Format s)"

docker desktop start 2>&1 | Out-String | Write-Output
"docker_started $(Get-Date -Format s)"
