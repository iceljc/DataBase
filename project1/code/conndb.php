<?php

$conn = new mysqli('localhost', 'root', '528627ljc', 'pickmeup');
if ($mysqli->connect_errno) {
    echo 'Failed to connect to MySQL: (' . $conn->connect_errno . ') ' . $mysqli->connect_error;
}
?>
