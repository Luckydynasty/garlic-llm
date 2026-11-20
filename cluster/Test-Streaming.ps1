$ErrorActionPreference='Stop'
Import-Module "$PSScriptRoot\Coordinator.psm1" -Force
$module=Get-Module Coordinator
$root=Join-Path ([IO.Path]::GetTempPath()) ('garlic-stream-test-'+[guid]::NewGuid().ToString('N'));$null=New-Item -ItemType Directory $root
function Assert($Condition,$Message){if(-not $Condition){throw $Message}}
& $module {
    $sample="data: {`"choices`" : [{`"index`":0,`"delta`":{`"content`":`"hello`"},`"finish_reason`":null}]}`n`n"+
        "data: {`"choices`" : [{`"index`":0,`"delta`":{},`"finish_reason`":`"stop`"}]}`n`ndata: [DONE]`n`n"
    $reader=New-Object IO.StringReader($sample)
    $result=Read-ClusterSse $reader {param($text)}
    if($result.choices[0].message.content -ne 'hello'){throw 'SSE aggregation failed.'}
    $reader=New-Object IO.StringReader($sample.Replace("data: [DONE]`n`n",''))
    $blocked=$false;try{Read-ClusterSse $reader {}}catch{$blocked=$true}
    if(-not $blocked){throw 'Truncated stream accepted.'}
    $reader=New-Object IO.StringReader(('x'*262145))
    $blocked=$false;try{Read-ClusterSse $reader {}}catch{$blocked=$true}
    if(-not $blocked){throw 'Unbounded line accepted.'}
}
$config=Join-Path $root 'nodes.json';$state=Join-Path $root 'state'
$nodes=@(
    @{id='alpha';address='192.168.77.3';port=8080;enabled=$true;apiVerified=$true;keyEnvironment='GARLIC_ALPHA_KEY'},
    @{id='beta';address='192.168.77.4';port=8080;enabled=$true;apiVerified=$true;keyEnvironment='GARLIC_BETA_KEY'})
@{nodes=$nodes} | ConvertTo-Json | Set-Content $config
$a=Submit-ClusterJob $config alpha 'alpha prompt' $state -ConversationId conversation-a -Stream
$b=Submit-ClusterJob $config beta 'beta prompt' $state -ConversationId conversation-b -Stream
$scriptPath=Join-Path $PSScriptRoot 'Coordinator.psm1'
$workers=@()
try{
    foreach($id in @('alpha','beta')){
        $workers+=Start-Job -ArgumentList $scriptPath,$config,$state,$root,$id -ScriptBlock {
            param($modulePath,$config,$state,$root,$id)
            $ErrorActionPreference='Stop'
            Import-Module $modulePath -Force
            [Environment]::SetEnvironmentVariable(('GARLIC_'+$id.ToUpper()+'_KEY'),'fixture-key')
            & (Get-Module Coordinator) {
                param($root)
                $script:fixtureRoot=$root
                function script:Invoke-ClusterHttp {
                    param($Node,$Method,$Path,$Body,$Timeout,$OnProgress)
                    $bodyObject=$Body | ConvertFrom-Json
                    if(-not $bodyObject.stream){throw 'Streaming was not requested.'}
                    [IO.File]::WriteAllText((Join-Path $script:fixtureRoot ($Node.id+'.entered')),'entered')
                    $end=[datetime]::UtcNow.AddSeconds(15)
                    while(@(Get-ChildItem $script:fixtureRoot -Filter '*.entered').Count -lt 2){if([datetime]::UtcNow -ge $end){throw 'Workers never overlapped.'};Start-Sleep -Milliseconds 50}
                    $content=$Node.id+' '+[char]0x00e9+' '+[char]0x4e2d
                    $first=@{choices=@(@{index=0;delta=@{content=$Node.id};finish_reason=$null})} | ConvertTo-Json -Depth 6 -Compress
                    $last=@{choices=@(@{index=0;delta=@{content=$content.Substring($Node.id.Length)};finish_reason='stop'})} | ConvertTo-Json -Depth 6 -Compress
                    $reader=New-Object IO.StringReader("data: $first`n`ndata: $last`n`ndata: [DONE]`n`n")
                    try{Read-ClusterSse $reader $OnProgress}finally{$reader.Dispose()}
                }
            } $root
            Invoke-ClusterWorker $config $id $state
        }
    }
    $null=Wait-Job $workers -Timeout 35
    foreach($w in $workers){if($w.State -ne 'Completed'){throw 'Concurrent worker failed or timed out.'}}
    $results=@($workers | Receive-Job -ErrorAction Stop)
    Assert ($results.Count -eq 2 -and @($results | Where-Object state -ne succeeded).Count -eq 0) 'Concurrent dispatch failed.'
    foreach($r in $results){Assert ($r.response.choices[0].message.content.StartsWith($r.node)) 'Cross-node response contamination.'}
    $progress=Get-ClusterProgress $state $a.id
    Assert ($progress.node -eq 'alpha' -and $progress.text.Contains([string][char]0x4e2d)) 'Unicode streaming progress missing.'
    $next=Submit-ClusterJob $config beta 'next question' $state -ConversationId conversation-a
    Assert ($next.messages.Count -eq 3 -and $next.messages[1].role -eq 'assistant' -and $next.messages[1].content.StartsWith('alpha')) 'Full conversation was not carried to new node.'
    $blocked=$false;try{Submit-ClusterJob $config alpha 'racing turn' $state -ConversationId conversation-a}catch{$blocked=$true}
    Assert $blocked 'Two pending turns in same conversation were allowed.'
    $blocked=$false;try{Submit-ClusterJob $config alpha ('x'*4000) $state -ConversationId budget-test}catch{$blocked=$true}
    Assert $blocked 'Oversize context accepted.'
    Assert (@(Get-ClusterJobs $state).Count -eq 3) 'Rejected context persisted a job.'
    'PASS: SSE aggregation/truncation/limits; two worker processes overlap; per-node Unicode progress/results; conversation carryover/isolation; pending-turn and context refusal. Simulated transports only.'
}finally{foreach($w in $workers){if($w.State -eq 'Running'){Stop-Job $w};Remove-Job $w -Force}}
