$ErrorActionPreference='Stop'
Import-Module "$PSScriptRoot\Coordinator.psm1" -Force
$module=Get-Module Coordinator
$root=Join-Path ([IO.Path]::GetTempPath()) ('garlic-network-test-'+[guid]::NewGuid().ToString('N'));$null=New-Item -ItemType Directory $root
$nodeExe=(Get-Command node -ErrorAction Stop).Source
$proc=Start-Process -FilePath $nodeExe -ArgumentList @(('"'+(Join-Path $PSScriptRoot 'SimulatedApi.cjs')+'"'),('"'+$root+'"')) -WindowStyle Hidden -PassThru
$oldKey=$env:GARLIC_NETWORK_KEY;$env:GARLIC_NETWORK_KEY='fixture-key'
function Assert($Condition,$Message){if(-not $Condition){throw $Message}}
try{
    $end=[datetime]::UtcNow.AddSeconds(15);$ready=Join-Path $root 'ready.json'
    while(-not(Test-Path $ready)){if($proc.HasExited -or [datetime]::UtcNow -gt $end){throw 'Fixture startup failed.'};Start-Sleep -Milliseconds 100}
    $ports=Get-Content $ready -Raw | ConvertFrom-Json
    $node=[pscustomobject]@{id='network';address='127.0.0.1';port=$ports[0];enabled=$true;apiVerified=$true;keyEnvironment='GARLIC_NETWORK_KEY'}
    # Direct private transport testing never relaxes Read-ClusterNodes validation.
    $progressFile=Join-Path $root 'callback.txt'
    $r=& $module {param($n,$p) Invoke-ClusterHttp $n GET '/good' '' 5 {param($text)[IO.File]::AppendAllText($p,($text+"`n"),(New-Object Text.UTF8Encoding($false)))}} $node $progressFile
    Assert ($r.choices[0].message.content.Contains([string][char]0x4e2d)) 'UTF-8 chunk decoding failed.'
    $updates=@(Get-Content $progressFile -Encoding UTF8)
    Assert ($updates.Count -eq 2 -and -not $updates[0].Contains('second') -and $updates[1].Contains('second')) 'Incremental network progress failed.'
    Assert ($r.choices[0].message.content.EndsWith(([string][char]0x4e2d)+[char]::ConvertFromUtf32(0x1f331))) 'Split multi-byte code points corrupted.'
    $workers=@()
    try{
        foreach($port in $ports){
            $workers+=Start-Job -ArgumentList (Join-Path $PSScriptRoot 'Coordinator.psm1'),$port -ScriptBlock {
                param($modulePath,$port)
                $ErrorActionPreference='Stop';Import-Module $modulePath -Force;$env:GARLIC_NETWORK_KEY='fixture-key'
                & (Get-Module Coordinator) {param($port) $n=[pscustomobject]@{address='127.0.0.1';port=$port;keyEnvironment='GARLIC_NETWORK_KEY'};Invoke-ClusterHttp $n GET '/concurrent' '' 10 {param($text)}} $port
            }
        }
        $null=Wait-Job $workers -Timeout 20
        Assert (@($workers | Where-Object State -ne Completed).Count -eq 0) 'Network concurrency stalled.'
        $answers=@($workers | Receive-Job -ErrorAction Stop)
        Assert ($answers.Count -eq 2 -and (Test-Path (Join-Path $root 'overlap'))) 'Two HTTP requests did not overlap.'
        Assert ($answers[0].choices[0].message.content -ne $answers[1].choices[0].message.content) 'Responses mixed across servers.'
    }finally{foreach($w in $workers){if($w.State -eq 'Running'){Stop-Job $w};Remove-Job $w -Force}}
    foreach($mode in @('headers-stall','body-stall','trickle','drop','invalid-utf8','redirect','busy')){
        $watch=[Diagnostics.Stopwatch]::StartNew();$failed=$false
        try{& $module {param($n,$mode) Invoke-ClusterHttp $n GET ('/'+$mode) '' 1 {param($t)}} $node $mode | Out-Null}catch{$failed=$true}
        Assert $failed "Fault accepted: $mode"
        Assert ($watch.Elapsed.TotalSeconds -lt 6) "Deadline ineffective: $mode"
    }
    $env:GARLIC_NETWORK_KEY='wrong';$failed=$false
    try{& $module {param($n) Invoke-ClusterHttp $n GET '/health'} $node}catch{$failed=$true}
    Assert $failed 'Wrong key accepted.';$env:GARLIC_NETWORK_KEY='fixture-key'
    # Queue integration: override ONLY enrollment in this test module instance.
    & $module {param($n) $script:fixtureNode=$n;function script:Read-ClusterNodes {param($Config) $script:fixtureNode}} $node
    $state=Join-Path $root 'state'
    $j=Submit-ClusterJob ignored network drop $state -Stream
    $r=Invoke-ClusterWorker ignored network $state -TimeoutSeconds 2
    Assert ($r.state -eq 'unknown') 'Socket drop did not create unknown job.'
    Assert ((Get-ClusterProgress $state $j.id).partial) 'Dropped stream lost partial progress.'
    $blocked=$false;try{Invoke-ClusterWorker ignored network $state}catch{$blocked=$true}
    Assert $blocked 'Unknown delivery allowed automatic replay.'
    $null=Resolve-ClusterJob $state $j.id ConfirmConsoleIdle 'Synthetic server disconnected; no remote work remains.'
    $next=Submit-ClusterJob ignored network good $state -Stream
    $r=Invoke-ClusterWorker ignored network $state -TimeoutSeconds 5
    Assert ($r.state -eq 'succeeded' -and -not (Get-ClusterProgress $state $next.id).partial) 'Recovery job failed.'
    $requests=@(Get-Content (Join-Path $root 'requests.jsonl') | ForEach-Object {$_ | ConvertFrom-Json})
    Assert (@($requests | Where-Object mode -eq redirect-target).Count -eq 0) 'Redirect followed.'
    Assert (@($requests | Where-Object mode -eq drop).Count -eq 2) 'Dropped request was replayed (expected one direct test and one queued test).'
    'PASS: real loopback HTTP chunked SSE/UTF-8/progress; header/body/trickle deadlines; disconnect/invalid bytes/redirect/503/401 refusal; persistent unknown, no replay, explicit recovery.'
}finally{$env:GARLIC_NETWORK_KEY=$oldKey;if(-not $proc.HasExited){Stop-Process -Id $proc.Id};$proc.Dispose()}
