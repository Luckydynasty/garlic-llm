# File locks work across Windows sessions. All workers MUST share StateDirectory.
function Open-ClusterLock([string]$Path) {
    $end=[datetime]::UtcNow
    if([IO.Path]::GetFileName($Path) -eq 'store.lock'){$end=$end.AddSeconds(5)}
    do {
        try { return [IO.File]::Open($Path,[IO.FileMode]::OpenOrCreate,[IO.FileAccess]::ReadWrite,[IO.FileShare]::None) }
        catch [IO.IOException] { if([datetime]::UtcNow -ge $end){throw 'Coordinator resource is locked by another process. Try again later.'};Start-Sleep -Milliseconds 25 }
    }while($true)
}
function Initialize-ClusterStore([string]$Directory) {
    $null=New-Item -ItemType Directory -Force -Path $Directory
    return [IO.Path]::GetFullPath($Directory)
}
function Write-ClusterRecord($Record,[string]$Path) {
    $temp=$Path+'.'+[guid]::NewGuid().ToString('N')+'.tmp'
    try {
        $bytes=(New-Object Text.UTF8Encoding($false)).GetBytes(($Record | ConvertTo-Json -Depth 30))
        $f=[IO.File]::Open($temp,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
        try{$f.Write($bytes,0,$bytes.Length);$f.Flush($true)}finally{$f.Dispose()}
        if([IO.File]::Exists($Path)){[IO.File]::Replace($temp,$Path,[NullString]::Value)}else{[IO.File]::Move($temp,$Path)}
    } finally { if([IO.File]::Exists($temp)){[IO.File]::Delete($temp)} }
}
function Read-ClusterRecords([string]$Directory) {
    foreach($f in Get-ChildItem -LiteralPath $Directory -Filter '*.json'){
        if($f.Name -notmatch '^[a-f0-9]{32}\.json$'){continue}
        try{
            $r=Get-Content -LiteralPath $f.FullName -Raw | ConvertFrom-Json
            if($r.version -ne 2 -or $r.id+'.json' -ne $f.Name -or $r.state -notin @('queued','running','succeeded','unknown','cancelled','resolved')){throw 'Invalid record schema.'}
            $r
        }catch{throw "Unreadable job record $($f.Name). Store is blocked until restored; records were not skipped."}
    }
}
function Add-ClusterEvent($Job,[string]$State,[string]$Note) {
    $Job.state=$State
    $Job.events=@($Job.events)+@([pscustomobject]@{time=[datetime]::UtcNow.ToString('o');state=$State;note=$Note})
}
function Get-ClusterJobs([string]$StateDirectory) {
    $dir=Initialize-ClusterStore $StateDirectory;$lock=Open-ClusterLock (Join-Path $dir 'store.lock')
    try{Read-ClusterRecords $dir | Sort-Object created,id}finally{$lock.Dispose()}
}
function Submit-ClusterJob([string]$Config,[string]$NodeId,[string]$Prompt,[string]$StateDirectory,[int]$MaxTokens=256,[double]$Temperature=0.7,[string]$ConversationId='', [switch]$Stream) {
    if([string]::IsNullOrWhiteSpace($Prompt) -or [Text.Encoding]::UTF8.GetByteCount($Prompt) -gt 32768){throw 'Prompt must contain 1-32768 UTF-8 bytes.'}
    if($MaxTokens -lt 1 -or $MaxTokens -gt 2048 -or [double]::IsNaN($Temperature) -or [double]::IsInfinity($Temperature) -or $Temperature -lt 0 -or $Temperature -gt 1.5){throw 'Invalid generation limits.'}
    $nodes=@(Read-ClusterNodes $Config | Where-Object id -eq $NodeId)
    if($nodes.Count -ne 1 -or -not $nodes[0].enabled -or -not $nodes[0].apiVerified){throw 'Node must be enabled and API-verified.'}
    $n=$nodes[0];$dir=Initialize-ClusterStore $StateDirectory;$lock=Open-ClusterLock (Join-Path $dir 'store.lock')
    try{
        $records=@(Read-ClusterRecords $dir)
        $context=4096
        if($n.PSObject.Properties['contextTokens']){
            if($n.contextTokens -isnot [int] -or $n.contextTokens -lt 1024 -or $n.contextTokens -gt 16384){throw 'Invalid verified contextTokens setting.'}
            $context=$n.contextTokens
        }
        $messages=Get-ClusterMessages $records $ConversationId $Prompt $MaxTokens $context
        $job=[pscustomobject]@{version=2;id=[guid]::NewGuid().ToString('N');node=$NodeId;address=$n.address;port=$n.port;created=[datetime]::UtcNow.ToString('o');state='queued';prompt=$Prompt;maxTokens=$MaxTokens;temperature=$Temperature;response=$null;events=@()}
        Add-ClusterEvent $job queued 'Persisted locally; not sent.'
        $job | Add-Member -NotePropertyName conversation -NotePropertyValue $ConversationId
        $job | Add-Member -NotePropertyName messages -NotePropertyValue @($messages)
        $job | Add-Member -NotePropertyName streaming -NotePropertyValue ([bool]$Stream)
        Write-ClusterRecord $job (Join-Path $dir ($job.id+'.json'))
        return $job
    }finally{$lock.Dispose()}
}
function Invoke-ClusterWorker([string]$Config,[string]$NodeId,[string]$StateDirectory) {
    $nodes=@(Read-ClusterNodes $Config | Where-Object id -eq $NodeId)
    if($nodes.Count -ne 1 -or -not $nodes[0].enabled -or -not $nodes[0].apiVerified){throw 'Node not enrolled.'}
    $n=$nodes[0];$dir=Initialize-ClusterStore $StateDirectory
    $endpointLock=Open-ClusterLock (Join-Path $dir ("endpoint-$($n.address)-$($n.port).lock"))
    try{
        $lock=Open-ClusterLock (Join-Path $dir 'store.lock')
        try{
            $records=@(Read-ClusterRecords $dir)
            $related=@($records | Where-Object { $_.node -eq $NodeId -or ($_.address -eq $n.address -and $_.port -eq $n.port) })
            if(@($related | Where-Object state -in @('running','unknown')).Count){throw 'Unresolved delivery exists. Inspect the console and use Resolve-ClusterJob; no replay is allowed.'}
            $queue=@($records | Where-Object { $_.node -eq $NodeId -and $_.state -eq 'queued' } | Sort-Object created,id)
            if(-not $queue.Count){return}
            $job=$queue[0]
            if($job.address -ne $n.address -or $job.port -ne $n.port){throw 'Queued endpoint differs from current configuration; cancel and explicitly resubmit.'}
            $context=4096
            if($n.PSObject.Properties['contextTokens']){
                if($n.contextTokens -isnot [int] -or $n.contextTokens -lt 1024 -or $n.contextTokens -gt 16384){throw 'Invalid verified contextTokens setting.'}
                $context=$n.contextTokens
            }
            $budget=64+$job.maxTokens
            $requestMessages=@(@{role='user';content=$job.prompt})
            if($job.PSObject.Properties['messages']){$requestMessages=@($job.messages)}
            foreach($m in $requestMessages){$budget += [Text.Encoding]::UTF8.GetByteCount($m.content)+32}
            if($budget -gt $context){throw 'Queued context exceeds current node budget; nothing sent.'}
            if([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($n.keyEnvironment))){throw 'Node key missing; job remains queued.'}
            Add-ClusterEvent $job running 'Dispatch claimed; crash after this point requires reconciliation.'
            $path=Join-Path $dir ($job.id+'.json');Write-ClusterRecord $job $path
        }finally{$lock.Dispose()}
        try{
            $messages=@(@{role='user';content=$job.prompt});$streaming=$false
            if($job.PSObject.Properties['messages']){$messages=@($job.messages)}
            if($job.PSObject.Properties['streaming']){$streaming=$job.streaming}
            $body=@{messages=$messages;max_tokens=$job.maxTokens;temperature=$job.temperature;stream=$streaming} | ConvertTo-Json -Depth 8
            $progress=$null
            if($streaming){
                $progress={param($text) Write-ClusterRecord @{job=$job.id;node=$job.node;partial=$true;text=$text;checked=[datetime]::UtcNow.ToString('o')} (Join-Path $dir ($job.id+'.progress'))}
            }
            $answer=Invoke-ClusterHttp $n POST '/v1/chat/completions' $body 180 $progress
            if(-not $answer.choices -or $answer.choices.Count -ne 1 -or $answer.choices[0].message.content -isnot [string] -or $answer.choices[0].finish_reason -notin @('stop','length')){throw 'Invalid completion.'}
            $job.response=$answer;Add-ClusterEvent $job succeeded 'Response received; length may mean token limit or controller stop.'
        }catch{Add-ClusterEvent $job unknown 'Delivery/completion uncertain. No retry. Inspect console before resolution.'}
        $lock=Open-ClusterLock (Join-Path $dir 'store.lock')
        try{
            Write-ClusterRecord $job $path
            $progressPath=Join-Path $dir ($job.id+'.progress')
            if($streaming -and $job.state -eq 'succeeded'){
                Write-ClusterRecord @{job=$job.id;node=$job.node;partial=$false;text=$job.response.choices[0].message.content;checked=[datetime]::UtcNow.ToString('o')} $progressPath
            }
        }finally{$lock.Dispose()}
        $job
    }finally{$endpointLock.Dispose()}
}
function Resolve-ClusterJob([string]$StateDirectory,[string]$JobId,[ValidateSet('CancelQueued','ConfirmConsoleIdle')][string]$Action,[string]$Note) {
    if($JobId -notmatch '^[a-f0-9]{32}$' -or [string]::IsNullOrWhiteSpace($Note)){throw 'Valid job ID and reconciliation note required.'}
    $dir=Initialize-ClusterStore $StateDirectory
    # Read once to obtain endpoint, then take locks in worker order and reread under store lock.
    $jobs=@(Get-ClusterJobs $dir | Where-Object id -eq $JobId)
    if($jobs.Count -ne 1){throw 'Job not found.'}
    $endpointLock=Open-ClusterLock (Join-Path $dir ("endpoint-$($jobs[0].address)-$($jobs[0].port).lock"))
    try{
        $lock=Open-ClusterLock (Join-Path $dir 'store.lock')
        try{
            $job=@(Read-ClusterRecords $dir | Where-Object id -eq $JobId)[0]
            if($Action -eq 'CancelQueued'){
                if($job.state -ne 'queued'){throw 'Only an unsent queued job can be cancelled locally.'}
                Add-ClusterEvent $job cancelled $Note
            }else{
                if($job.state -notin @('running','unknown')){throw 'Only unresolved work can be reconciled.'}
                Add-ClusterEvent $job resolved ('Operator confirmed console idle; original outcome remains unconfirmed. '+$Note)
            }
            Write-ClusterRecord $job (Join-Path $dir ($job.id+'.json'));$job
        }finally{$lock.Dispose()}
    }finally{$endpointLock.Dispose()}
}
function Get-ClusterHealth([string]$Config,[string]$StateDirectory) {
    $dir=Initialize-ClusterStore $StateDirectory
    foreach($n in Read-ClusterNodes $Config){
        $state='Disabled';$lock=$null
        if($n.enabled -and $n.apiVerified){
            try{
                $lock=Open-ClusterLock (Join-Path $dir ("endpoint-$($n.address)-$($n.port).lock"))
                $unresolved=@(Get-ClusterJobs $dir | Where-Object {($_.node -eq $n.id -or $_.address -eq $n.address) -and $_.state -in @('running','unknown')})
                if($unresolved.Count){$state='Needs reconciliation'}else{$r=Invoke-ClusterHttp $n GET '/health';if($r.status -eq 'ok'){$state='API reachable'}else{$state='Unexpected response'}}
            }catch{$state='Not confirmed (busy, unavailable or store blocked)'}finally{if($lock){$lock.Dispose()}}
        }
        [pscustomobject]@{Node=$n.id;State=$state;Checked=[datetime]::UtcNow.ToString('o')}
    }
}
# Compatibility convenience; deliberately not a background service.
function Invoke-ClusterJob([string]$Config,[string]$NodeId,[string]$Prompt,[string]$StateDirectory,[int]$MaxTokens=256,[double]$Temperature=0.7){
    $queued=Submit-ClusterJob $Config $NodeId $Prompt $StateDirectory $MaxTokens $Temperature
    [pscustomobject]@{SubmittedJob=$queued.id;ProcessedJob=(Invoke-ClusterWorker $Config $NodeId $StateDirectory)}
}
