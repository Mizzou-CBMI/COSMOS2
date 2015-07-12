<?php
$path = './downloads/COSMOS-1.0.0.tar.gz'; 
if ($_POST['agree']=='Yes') 
{
    header("Cache-Control: must-revalidate, post-check=0, pre-check=0");
    header("Cache-Control: private");
    header("Content-Description: File Transfer");
    header("Content-Type: " . "application/octet-stream");
    header("Content-Length: " .(string)(filesize($path)) );
    header('Content-Disposition: attachment; filename="'.basename($path).'"');
    header("Content-Transfer-Encoding: binary\n");
    readfile($path); 
    exit();
}
else {
    header( 'Location: http://cosmos.hms.harvard.edu' );
}
?>
