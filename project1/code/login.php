<?php
    require_once "conndb.php";
    session_start();
    if (isset($_POST['username'])){
        $username = $_POST['username'];
        $password = $_POST['password'];
        
        $query = "SELECT * FROM users WHERE username='".$username."' AND password='".$password."'";
        $res = mysqli_query($conn, $query);
        $rows = $res->num_rows;
        if ($rows == 1){
            $_SESSION['username'] = $username;
            header("Location: home.php");
        }else{
            echo "<div class='form'>
            <h3><p align='center'>Username or password is incorrect.</p></h3></div>";
        }
    }
?>


<!DOCTYPE html>
<html>
<head>
<meta charset = "UTF-8">
<link rel="stylesheet" href="css/login.css" />
<style>
html {
height: 100%;
    box-sizing: border-box;
}
</style>
</head>
<body>

<div class="login">
<h1>Log In</h1>
<form action="" method="post" name="login">
<input id="username" type="text" name="username" placeholder="Username" required />
<input id="password" type="password" name="password" placeholder="Password" required />
<input name="submit" type="submit" value="Login" />
</form>
<h1>Not registered yet? <a href='register.php'>Register Here</a></h1>
</div>

</body>
</html>

