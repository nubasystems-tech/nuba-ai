# Nuba Laptop Worker — التثبيت كمهمة مجدولة تعمل عند الإقلاع
# المصدر: الخادم يزود الملفات عبر Tailscale

$ErrorActionPreference = "Stop"
$dir = "C:\NubaAI\worker"
New-Item -ItemType Directory -Force -Path $dir | Out-Null

# 1) تنزيل العامل من الخادم عبر Tailscale
$base = "http://100.77.139.53:8000"
Invoke-WebRequest -UseBasicParsing -TimeoutSec 30 -Headers @{ "X-Worker-Token"="nuba-worker-2026" } -OutFile "$dir\agent_worker.py" "$base/agent_worker.py"
Write-Host "✓ agent_worker.py نُزل"

# 2) سكربت التشغيل الصامت
Set-Content -Path "$dir\run_worker.cmd" -Value "@echo off`r`ncd /d C:\NubaAI\worker`r`npython agent_worker.py >> C:\NubaAI\worker\worker.log 2>&1"
Write-Host "✓ run_worker.cmd جاهز"

# 3) تسجيل مهمة مجدولة تعمل عند الإقلاع وتعيد نفسها كل 5 دقائق إن ماتت
$action = New-ScheduledTaskAction -Execute "$dir\run_worker.cmd"
$trigger = (New-ScheduledTaskTrigger -AtLogon), (New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 5))
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 10 -RestartInterval (New-TimeSpan -Minutes 2) -ExecutionTimeLimit (New-TimeSpan -Hours 0)
Register-ScheduledTask -TaskName "NubaWorker" -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null
Write-Host "✓ مهمة NubaWorker مسجلة (إقلاع + إعادة تشغيل تلقائية)"

# 4) تشغيل فوري للاختبار
Start-ScheduledTask -TaskName "NubaWorker"
Write-Host "🚀 العامل يعمل الآن — سيتصل بالخادم خلال 30 ثانية"
