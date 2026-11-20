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
    try{
        $reader=New-Object IO.StreamReader($response.GetResponseStream())
        try{
            $buffer=New-Object char[] 4096;$text=New-Object Text.StringBuilder
            $deadline=[datetime]::UtcNow.AddSeconds($Timeout)
            while(($read=$reader.Read($buffer,0,$buffer.Length)) -gt 0){
                if($text.Length+$read -gt 2097152 -or [datetime]::UtcNow -gt $deadline){throw 'Response exceeds read limits.'}
                $null=$text.Append($buffer,0,$read)
            }
            $text.ToString() | ConvertFrom-Json
        }finally{$reader.Dispose()}
    }
    finally{$response.Dispose()}
}
. (Join-Path $PSScriptRoot 'Queue.ps1')
Export-ModuleMember -Function Read-ClusterNodes,Get-ClusterHealth,Invoke-ClusterJob,Submit-ClusterJob,Get-ClusterJobs,Invoke-ClusterWorker,Resolve-ClusterJob
