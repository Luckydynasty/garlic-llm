Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
function Read-ClusterNodes([string]$Config) {
    $items=@((Get-Content -LiteralPath $Config -Raw | ConvertFrom-Json).nodes)
    $ids=@{}; $endpoints=@{}
    foreach($n in $items){
        if($n.id -notmatch '^[a-z0-9-]{1,48}$' -or $ids.ContainsKey($n.id)){throw 'Invalid or duplicate node ID.'}
        $ip=$null
        if(-not [Net.IPAddress]::TryParse($n.address,[ref]$ip) -or $ip.AddressFamily -ne [Net.Sockets.AddressFamily]::InterNetwork -or $n.address -notlike '192.168.77.*' -or $ip.GetAddressBytes()[3] -in @(0,1,254,255)){throw 'Node must use an allowed host on the dedicated 192.168.77.0/24 segment.'}
        # The address and port also identify locks and unresolved jobs. Alternate
        # spellings of one endpoint must never create a second ownership identity.
        if($n.address -cne $ip.ToString()){throw 'Node address must use canonical dotted-decimal IPv4 notation.'}
        if($n.port -isnot [int] -or $n.port -ne 8080){throw 'Only the reviewed Garlic API port 8080, as a JSON integer, is allowed.'}
        if($n.enabled -isnot [bool] -or $n.apiVerified -isnot [bool]){throw 'Capability flags must be JSON booleans.'}
        if($n.keyEnvironment -notmatch '^GARLIC_[A-Z0-9_]+_KEY$'){throw 'Invalid key environment variable name.'}
        $endpoint="$($n.address):$($n.port)"
        if($endpoints.ContainsKey($endpoint)){throw 'Duplicate node endpoint.'}
        $ids[$n.id]=$true; $endpoints[$endpoint]=$true
        $n
    }
}
. (Join-Path $PSScriptRoot 'Http.ps1')
. (Join-Path $PSScriptRoot 'Queue.ps1')
. (Join-Path $PSScriptRoot 'Streaming.ps1')
Export-ModuleMember -Function Read-ClusterNodes,Get-ClusterHealth,Invoke-ClusterJob,Submit-ClusterJob,Get-ClusterJobs,Invoke-ClusterWorker,Resolve-ClusterJob,Get-ClusterProgress
