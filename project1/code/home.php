<?php
    require_once "conndb.php";
    session_start();
?>

<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<link rel="stylesheet" href="css/style.css" />
<title>Welcome Home</title>

<style>


</style>
</head>
<body>
<div class="form">
<ul>
<li><a href="home.php">PickMeUp</a></li>
<li><a href="home.php">Home</a></li>
<li><a href="view.php">Book a Ride</a></li>
<li><a href="history.php">My Booked Ride</a></li>
<li><a href="post.php">Post a Ride</a></li>
<li><a href="cancelpost.php">My Posted Ride</a></li>
<li style="float:right"><a href="logout.php">Logout</a></li>
</ul>
<h1 align="center">Welcome to PickMeUp</h1>

<p align="center"><strong>PickMeUp</strong> is a National Car Sharing Platform</p>
<p align="center">Feel free to post and book your ride on <strong>PickMeUp</strong>.</p>
<p align="center">You can <strong>post/book/update/cancel</strong> your ride.</p>
</div>

</body>
</html>

