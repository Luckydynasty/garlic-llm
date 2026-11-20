$ErrorActionPreference='Stop'
Import-Module "$PSScriptRoot\Coordinator.psm1" -Force
$root=Join-Path ([IO.Path]::GetTempPath()) ('garlic-cluster-test-'+[guid]::NewGuid().ToString('N'))
$null=New-Item -ItemType Directory $root
$config=Join-Path $root 'nodes.json'
$node=@{id='test-node';address='192.168.77.3';port=8080;enabled=$true;apiVerified=$true;keyEnvironment='GARLIC_TEST_KEY'}
function Save-Config {@{nodes=@($node)} | ConvertTo-Json | Set-Content $config}
Save-Config
$oldKey=$env:GARLIC_TEST_KEY;$env:GARLIC_TEST_KEY='synthetic-test-only'
$module=Get-Module Coordinator
function Assert($Condition,$Message){if(-not $Condition){throw $Message}}
function Assert-Blocked([scriptblock]$Code,$Message){$blocked=$false;try{& $Code | Out-Null}catch{$blocked=$true};Assert $blocked $Message}
try{
    & $module { function script:Invoke-ClusterHttp {param($Node,$Method,$Path,$Body,$Timeout);[pscustomobject]@{choices=@(@{message=@{content='Unicode answer: cafe'};finish_reason='stop'})}} }
    $store=Join-Path $root 'state'
    $a=Submit-ClusterJob $config test-node 'first' $store
    $b=Submit-ClusterJob $config test-node 'second' $store
    Assert (@(Get-ClusterJobs $store).Count -eq 2) 'Queue persistence failed.'
    Assert ((Invoke-ClusterWorker $config test-node $store).id -eq $a.id) 'FIFO failed.'
    $null=Resolve-ClusterJob $store $b.id CancelQueued 'User cancelled unsent work.'
    Assert (@(Invoke-ClusterWorker $config test-node $store).Count -eq 0) 'Cancelled job sent.'
    & $module {function script:Invoke-ClusterHttp {throw 'simulated ambiguous disconnect'}}
    $c=Submit-ClusterJob $config test-node 'third' $store
    Assert ((Invoke-ClusterWorker $config test-node $store).state -eq 'unknown') 'Unknown classification failed.'
    $d=Submit-ClusterJob $config test-node 'fourth' $store
    Assert-Blocked {Invoke-ClusterWorker $config test-node $store} 'Unknown job permitted replay.'
    $ready=Join-Path $root 'ready';$release=Join-Path $root 'release'
    $holder=Start-Job -ArgumentList $store,$ready,$release -ScriptBlock {
        param($store,$ready,$release)
        $f=[IO.File]::Open((Join-Path $store 'endpoint-192.168.77.3-8080.lock'),'OpenOrCreate','ReadWrite','None')
        try{[IO.File]::WriteAllText($ready,'ready');$end=[datetime]::UtcNow.AddSeconds(15);while(-not(Test-Path $release) -and [datetime]::UtcNow -lt $end){Start-Sleep -Milliseconds 100}}finally{$f.Dispose()}
    }
    $end=[datetime]::UtcNow.AddSeconds(10)
    while(-not(Test-Path $ready) -and [datetime]::UtcNow -lt $end){Start-Sleep -Milliseconds 100}
    Assert (Test-Path $ready) 'Lock test process did not start.'
    Assert-Blocked {Resolve-ClusterJob $store $c.id ConfirmConsoleIdle 'Not actually idle'} 'Active worker was reconciled.'
    [IO.File]::WriteAllText($release,'release');$null=Wait-Job $holder -Timeout 10;Remove-Job $holder -Force
    $null=Resolve-ClusterJob $store $c.id ConfirmConsoleIdle 'Simulated console inspected and idle.'
    $record=@(Get-ClusterJobs $store | Where-Object id -eq $c.id)[0]
    Assert ($record.state -eq 'resolved' -and $record.events.Count -eq 4) 'Reconciliation audit failed.'
    $path=Join-Path $store ($c.id+'.json');$record.state='running';$record | ConvertTo-Json -Depth 15 | Set-Content $path
    $node.id='renamed';Save-Config
    Assert-Blocked {Invoke-ClusterWorker $config renamed $store} 'Renamed endpoint bypassed recovery gate.'
    $node.id='test-node';$node.address='192.168.77.4';Save-Config
    Assert-Blocked {Invoke-ClusterWorker $config test-node $store} 'Node relocation bypassed unresolved work.'
    $node.address='192.168.77.3';Save-Config
    $null=Resolve-ClusterJob $store $c.id ConfirmConsoleIdle 'Recovered after simulated process crash.'
    $node.address='192.168.77.4';Save-Config
    Assert-Blocked {Invoke-ClusterWorker $config test-node $store} 'Queued work silently moved endpoints.'
    $node.address='8.8.8.8';Save-Config
    Assert-Blocked {Read-ClusterNodes $config} 'Public endpoint allowed.'
    $node.address='192.168.77.3';Save-Config
    Assert-Blocked {Submit-ClusterJob $config test-node x $store 256 ([double]::PositiveInfinity)} 'Infinite temperature allowed.'
    $bad=Join-Path $store ('a'*32+'.json');[IO.File]::WriteAllText($bad,'{bad')
    Assert-Blocked {Invoke-ClusterWorker $config test-node $store} 'Corrupt record silently skipped.'
    'PASS: FIFO, cancellation, uncertain delivery, audit, cross-process locks, crash recovery, endpoint identity, relocation, input validation and corrupt-store refusal. Mock transport only.'
    "Fixture directory: $root"
}finally{$env:GARLIC_TEST_KEY=$oldKey}
