<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Registration</title>
<link rel="stylesheet" href="css/register.css" />
<style>
html {
height: 100%;
    box-sizing: border-box;
}

</style>
</head>
<body>
<?php
    require_once "conndb.php";
    // If form submitted, insert values into the database.
    if (isset($_REQUEST['username'])){
        
        $username = $_REQUEST['username'];
        $email = $_REQUEST['email'];
        $password = $_REQUEST['password'];
        $trn_date = date("Y-m-d H:i:s");
        
        $check_query = "SELECT username FROM users WHERE username='".$username."'";
        $check_res = mysqli_query($conn, $check_query);
        
        $check_flag = 0;
        if ($check_res->num_rows>0){
            $check_flag = 1;
        }
        
        if($check_flag != 0){
            echo "<div class='login'>
            <h3><p align='center'>The username has been registered.</p></h3>
            <br/><p align='center'>Click here to <a href='register.php'>Register</a></p></div>";
        }else{
            $query = "INSERT into `users` (username, password, email, trn_date) VALUES ('$username', '$password', '$email', '$trn_date')";
            $result = mysqli_query($conn,$query);
            if($result){
                echo "<div class='login'>
                <h3><p align='center'>You are registered successfully.</p></h3>
                <br/><p align='center'>Click here to <a href='login.php'>Login</a></p></div>";
            }
        }
        
        
    }else{
?>
<div class="login">
<h1>Registration</h1>
<form name="registration" action="" method="post">
<input id="username" type="text" name="username" placeholder="Username" required />
<input id="password" type="password" name="password" placeholder="Password" required />
<input id="email" type="text" name="email" placeholder="Email" required />
<input type="submit" name="submit" value="Register" />
<h1><p align="center"><a href="login.php">Click here to Login</a></p></h1>
</form>
</div>
<?php } ?>

</body>
</html>

