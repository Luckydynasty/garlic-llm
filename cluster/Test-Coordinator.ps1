$ErrorActionPreference='Stop'
Import-Module "$PSScriptRoot\Coordinator.psm1" -Force
$root=Join-Path ([IO.Path]::GetTempPath()) ('garlic-cluster-test-'+[guid]::NewGuid().ToString('N'))
$null=New-Item -ItemType Directory $root
$config=Join-Path $root 'nodes.json'
$node=@{id='test-node';address='192.168.77.3';port=8080;enabled=$true;apiVerified=$true;keyEnvironment='GARLIC_TEST_KEY'}
@{nodes=@($node)} | ConvertTo-Json | Set-Content $config
$env:GARLIC_TEST_KEY='synthetic-test-only'
try{
    $module=Get-Module Coordinator
    & $module { function script:Invoke-ClusterHttp {param($Node,$Method,$Path,$Body,$Timeout);[pscustomobject]@{choices=@(@{message=@{content='mock answer'};finish_reason='stop'})}} }
    $r=Invoke-ClusterJob $config test-node 'test' (Join-Path $root 'ok')
    if($r.State -ne 'succeeded'){throw 'Success persistence failed.'}
    & $module {function script:Invoke-ClusterHttp {throw 'simulated ambiguous disconnect'}}
    $r=Invoke-ClusterJob $config test-node 'test' (Join-Path $root 'uncertain')
    if($r.State -ne 'unknown'){throw 'Unknown classification failed.'}
    $blocked=$false
    try{Invoke-ClusterJob $config test-node 'retry' (Join-Path $root 'uncertain')}catch{$blocked=$true}
    if(-not $blocked){throw 'Unknown job was replayed.'}
    $node.address='8.8.8.8';@{nodes=@($node)} | ConvertTo-Json | Set-Content $config
    $blocked=$false;try{Read-ClusterNodes $config}catch{$blocked=$true}
    if(-not $blocked){throw 'Unsafe destination accepted.'}
    'PASS: persistence, ambiguous delivery, replay blocking and destination validation (mock transport only).'
}finally{Remove-Item Env:GARLIC_TEST_KEY -ErrorAction SilentlyContinue}
