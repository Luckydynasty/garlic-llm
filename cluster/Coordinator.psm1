Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
function Read-ClusterNodes([string]$Config) {
    $items=@((Get-Content -LiteralPath $Config -Raw | ConvertFrom-Json).nodes)
    $ids=@{}; $endpoints=@{}
    foreach($n in $items){
        if($n.id -notmatch '^[a-z0-9-]{1,48}$' -or $ids.ContainsKey($n.id)){throw 'Invalid or duplicate node ID.'}
        $ip=$null
        if(-not [Net.IPAddress]::TryParse($n.address,[ref]$ip) -or $ip.AddressFamily -ne [Net.Sockets.AddressFamily]::InterNetwork -or $n.address -notlike '192.168.77.*' -or $ip.GetAddressBytes()[3] -in @(0,1,254,255)){throw 'Node must use an allowed host on the dedicated 192.168.77.0/24 segment.'}
        if($n.port -ne 8080){throw 'Only the reviewed Garlic API port 8080 is allowed.'}
        if($n.enabled -isnot [bool] -or $n.apiVerified -isnot [bool]){throw 'Capability flags must be JSON booleans.'}
        if($n.keyEnvironment -notmatch '^GARLIC_[A-Z0-9_]+_KEY$'){throw 'Invalid key environment variable name.'}
        $endpoint="$($n.address):$($n.port)"
        if($endpoints.ContainsKey($endpoint)){throw 'Duplicate node endpoint.'}
        $ids[$n.id]=$true; $endpoints[$endpoint]=$true
        $n
    }
}
function Invoke-ClusterHttp($Node,[string]$Method,[string]$Path,[string]$Body='',[int]$Timeout=5) {
    $key=[Environment]::GetEnvironmentVariable($Node.keyEnvironment)
    if([string]::IsNullOrWhiteSpace($key)){throw 'Set the node API key environment variable before connecting.'}
    $request=[Net.HttpWebRequest]::Create("http://$($Node.address):$($Node.port)$Path")
    $request.Proxy=$null; $request.AllowAutoRedirect=$false; $request.Method=$Method
    $request.Timeout=$Timeout*1000; $request.ReadWriteTimeout=$Timeout*1000
    $request.Headers['Authorization']='Bearer '+$key
    if($Method -eq 'POST'){
        $bytes=[Text.Encoding]::UTF8.GetBytes($Body)
        $request.ContentType='application/json';$request.ContentLength=$bytes.Length
        $stream=$request.GetRequestStream();try{$stream.Write($bytes,0,$bytes.Length)}finally{$stream.Dispose()}
    }
    $response=$request.GetResponse()
    try{$reader=New-Object IO.StreamReader($response.GetResponseStream());try{$reader.ReadToEnd() | ConvertFrom-Json}finally{$reader.Dispose()}}
    finally{$response.Dispose()}
}
function Get-ClusterHealth([string]$Config){
    foreach($n in Read-ClusterNodes $Config){
        $state='Disabled'
        if($n.enabled -and $n.apiVerified){
            $m=New-Object Threading.Mutex($false,('Local\GarlicCluster-'+$n.id));$held=$false
            try{
                try{$held=$m.WaitOne(0)}catch [Threading.AbandonedMutexException]{$held=$true}
                if(-not $held){$state='Busy locally'}
                else{try{$r=Invoke-ClusterHttp $n GET '/health';if($r.status -eq 'ok'){$state='API reachable'}else{$state='Unexpected health response'}}catch{$state='Not confirmed (offline, busy, auth or timeout)'}}
            }finally{if($held){$m.ReleaseMutex()};$m.Dispose()}
        }
        [pscustomobject]@{Node=$n.id;State=$state;Checked=(Get-Date -Format o)}
    }
}
function Invoke-ClusterJob([string]$Config,[string]$NodeId,[string]$Prompt,[string]$StateDirectory,[int]$MaxTokens=256,[double]$Temperature=0.7){
    if([string]::IsNullOrWhiteSpace($Prompt) -or [Text.Encoding]::UTF8.GetByteCount($Prompt) -gt 32768){throw 'Prompt must contain 1-32768 UTF-8 bytes.'}
    if($MaxTokens -lt 1 -or $MaxTokens -gt 2048 -or [double]::IsNaN($Temperature) -or $Temperature -lt 0 -or $Temperature -gt 1.5){throw 'Invalid generation limits.'}
    $nodes=@(Read-ClusterNodes $Config | Where-Object id -eq $NodeId)
    if($nodes.Count -ne 1 -or -not $nodes[0].enabled -or -not $nodes[0].apiVerified){throw 'Node must be explicitly enabled and API-verified.'}
    $n=$nodes[0];$mutex=New-Object Threading.Mutex($false,('Local\GarlicCluster-'+$n.id));$held=$false
    try{
        try{$held=$mutex.WaitOne(0)}catch [Threading.AbandonedMutexException]{$held=$true}
        if(-not $held){throw 'Node is already owned by another local cluster operation.'}
        $null=New-Item -ItemType Directory -Force $StateDirectory
        # Uncertain/running jobs survive process crashes and require manual reconciliation.
        foreach($f in Get-ChildItem -LiteralPath $StateDirectory -Filter '*.json'){
            $prior=Get-Content -LiteralPath $f.FullName -Raw | ConvertFrom-Json
            if($prior.node -eq $NodeId -and $prior.state -in @('running','unknown')){throw 'An unresolved job exists for this node. Inspect the PS4 and reconcile it before submitting another.'}
        }
        if(-not [Environment]::GetEnvironmentVariable($n.keyEnvironment)){throw 'API key is missing.'}
        $job=[ordered]@{id=[guid]::NewGuid().ToString('N');node=$NodeId;state='running';created=(Get-Date -Format o);prompt=$Prompt;response=$null;note=''}
        $file=Join-Path $StateDirectory ($job.id+'.json')
        function Save-Job { $temp=$file+'.tmp';[IO.File]::WriteAllText($temp,($job | ConvertTo-Json -Depth 15),(New-Object Text.UTF8Encoding($false)));Move-Item -LiteralPath $temp -Destination $file -Force }
        Save-Job
        try{
            $body=@{messages=@(@{role='user';content=$Prompt});max_tokens=$MaxTokens;temperature=$Temperature;stream=$false} | ConvertTo-Json -Depth 8
            $answer=Invoke-ClusterHttp $n POST '/v1/chat/completions' $body 180
            if(-not $answer.choices -or $null -eq $answer.choices[0].message.content){throw 'Invalid response.'}
            $job.response=$answer;$job.state='succeeded'
        }catch{
            $job.state='unknown';$job.note='Delivery or completion could not be confirmed. No retry or failover was attempted. Inspect the PS4 before reconciling this job.'
        }
        Save-Job
        [pscustomobject]@{Job=$job.id;Node=$NodeId;State=$job.state;File=$file}
    }finally{if($held){$mutex.ReleaseMutex()};$mutex.Dispose()}
}
Export-ModuleMember -Function Read-ClusterNodes,Get-ClusterHealth,Invoke-ClusterJob
