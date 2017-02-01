#Requires -Version 3.0
#Requires -Module AzureRM.Resources
#Requires -Module Azure.Storage

Param(
    [string] [Parameter(Mandatory=$true)] $ResourceGroupLocation,
    [string] $ResourceGroupName,
    [switch] $UploadArtifacts,
    [string] $StorageAccountName,
    [string] $StorageContainerName = $ResourceGroupName.ToLowerInvariant() + '-stageartifacts',
    [string] $TemplateFile, # = '..\Templates\azuredeploy.json',
    [string] $TemplateParametersFile, # = '..\Templates\azuredeploy.parameters.json',
    [string] $ArtifactStagingDirectory = '..\bin\Debug\staging',
    [string] $DSCSourceFolder = '..\DSC'
)

Write-Host "Executing Post-Deployment Scripts ..."

# Set our $StorageAccount-variable to the name of the StorageAccount of the LinkedService
$StorageAccount = (Get-AzureRmStorageAccount | Where-Object{$_.StorageAccountName -eq "gbdomaindata"})
# Copy files from the local storage staging location to the storage account container
# Create Container if not exists, use previously set $StorageAccount
New-AzureStorageContainer -Name "adfcontainer" -Context $StorageAccount.Context -ErrorAction SilentlyContinue *>&1
Set-AzureStorageBlobContent -File "D:\Work\SourceControl\GitHub\Azure.DataFactory.LocalEnvironment\MyADFProject\Dependencies\MyCustomActivity.zip" -Blob "package/MyCustomActivity.zip" -Container "adfcontainer" -Context $StorageAccount.Context -Force



Write-Host "Finished executing Post-Deployment Scripts!"