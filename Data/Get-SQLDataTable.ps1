function Get-SqlToDataTable
{
<#
    .EXAMPLE
    $DataTable = Get-SqlToDataTable -SQLServer servername -SQLDB dev -Target dbo.tableName
    .PARAMETER SQLServer
    Named instances serverName\instanceName | If it's a default instance only use server name, do not add \MSSQLSERVER.
    .PARAMETER SQLDB
    Name of the database where the target table is.
    .PARAMETER Target
    The Table in which you're targeting
    .PARAMETER Filter
    Default="*" Used in selecting the columns from the target.
    .PARAMETER Security
    Default="SSPI" Used in connection string to SQL DB.
    .PARAMETER Timeout
    Default="60" Used in connection string.
    .PARAMETER Where
    Variable in command text after Select * From Target. Example: "WHERE [Parent Container] LIKE '$($domain)%' AND [In AD] = '1' AND FQDN LIKE '%VCEN%'"
#>
param(
    [Parameter(Mandatory=$true)][string]$SQLServer,
    [Parameter(Mandatory=$true)][string]$SQLDB,
    [Parameter(Mandatory=$true)][string]$Target,
    [string]$Filter= "*",
    [string]$Security="SSPI",
    [Int]$Timeout="60",
    [string]$Where,
    [switch]$Distinct
)
begin {
#Create connection object to SQL instance
    $DataTable = $null
    if ($SQLConnection.State -ne [Data.ConnectionState]::Open) {
        $SQLConnection = New-Object System.Data.SqlClient.SqlConnection
        $SQLConnection.ConnectionString = "Server=$SQLServer;Database=$SQLDB;Integrated Security=$Security;Connection Timeout=$Timeout"
        $SQLConnection.Open()
    }
}
process {
    if ($SQLConnection.State -ne [Data.ConnectionState]::Open){"Connection to SQL DB not open"}
    else
    {
        #Command to be executed
        $SQLCommand = New-Object System.Data.SqlClient.SqlCommand
        $SQLCommand.Connection = $SQLConnection
        if($Distinct){$SQLCommand.CommandText = "SELECT DISTINCT $Filter FROM $Target $Where"}
        else{$SQLCommand.CommandText = "SELECT $Filter FROM $Target $Where"}
        #Empty DataTable to be filled by SQL Adapter
        $DataTable = New-Object System.Data.DataTable
        #SQL Adapter used to execute SQL command
        $SQLAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
        $SQLAdapter.SelectCommand = $SQLCommand
        $SQLAdapter.Fill($DataTable) | Out-Null
        Return ,$DataTable
    }
}
end{
        #Closing connection to SQL server if open
        if ($SQLConnection.State -eq [Data.ConnectionState]::Open) {
            $SQLConnection.Close()
        }
    }
}
