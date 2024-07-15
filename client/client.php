<?php

$metrics_file_path = "metrics.json";
$metrics = file_get_contents($metrics_file_path);

$fp = stream_socket_client("unix://../metric-server.sock", $errno, $errstr, 30);
if (!$fp) {
    echo "$errstr ($errno)";
} else {
    fwrite($fp, $metrics);
    fclose($fp);
}
?>
