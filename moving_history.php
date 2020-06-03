<?php
$servername = "localhost";
$username = "root";
$dbname = "mydb";

// Create connection
$conn = new mysqli($servername, $username, $dbname);
// Check connection
if ($conn->connect_error) {
    die("Connection failed: " . $conn->connect_error);
}

$sql = "SELECT something FROM somthing";
$result = $conn->query($sql);

?>