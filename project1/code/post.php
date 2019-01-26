<?php
    require_once "conndb.php";
    session_start();
    $status = "";
    if(isset($_POST['source']))
    {
        $fromCity = $_REQUEST['source'];
        $toCity = $_REQUEST['destination'];
        $carMake = $_REQUEST['make'];
        $carModel = $_REQUEST['model'];
        $submittedby = $_SESSION["username"];
        $cost = $_REQUEST['cost'];
        $booked = 0;
        
        $new_date = date("Y-m-d", strtotime($_POST['dateFrom']));
        $ins_query="INSERT INTO Ride (name_user,source,destination,car_make, car_model, start_date,cost,booked) VALUES ('$submittedby','$fromCity','$toCity','$carMake','$carModel','$new_date','$cost', '$booked')";
        if(mysqli_query($conn,$ins_query))
        {
            $status = "New Record Inserted Successfully.
            </br></br><a href='view.php'>View Rides</a>";
        }
    }
?>

<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Post a New Ride</title>
<link rel="stylesheet" href="css/style.css" />
<style>
<link href='http://fonts.googleapis.com/css?family=Open+Sans+Condensed:300' rel='stylesheet' type='text/css'>
<style type="text/css">
.form-style-8{
    font-family: 'Open Sans Condensed', arial, sans;
    width: 300px;
    padding: 30px;
    background: #FFFFFF;
    margin: 500px auto;
    box-shadow: 0px 0px 15px rgba(0, 0, 0, 0.22);
    -moz-box-shadow: 0px 0px 15px rgba(0, 0, 0, 0.22);
    -webkit-box-shadow:  0px 0px 15px rgba(0, 0, 0, 0.22);
}
.form-style-8 h2{
    background: #4D4D4D;
    text-transform: uppercase;
    font-family: 'Open Sans Condensed', sans-serif;
    color: #797979;
    font-size: 18px;
    font-weight: 100;
    padding: 20px;
    margin: -30px -30px 30px -30px;
}
.form-style-8 input[type="text"],
.form-style-8 input[type="date"],
.form-style-8 input[type="datetime"],
.form-style-8 input[type="email"],
.form-style-8 input[type="number"],
.form-style-8 input[type="search"],
.form-style-8 input[type="time"],
.form-style-8 input[type="url"],
.form-style-8 input[type="password"],
.form-style-8 textarea,
.form-style-8 select
{
    box-sizing: border-box;
    -webkit-box-sizing: border-box;
    -moz-box-sizing: border-box;
    outline: none;
    display: block;
    width: 20%;
    padding: 7px;
    border: none;
    border-bottom: 1px solid #ddd;
    background: transparent;
    margin-bottom: 1px;
    font: 16px Arial, Helvetica, sans-serif;
    height: 25px;
}
.form-style-8 textarea{
    resize:none;
    overflow: hidden;
}
.form-style-8 input[type="button"],
.form-style-8 input[type="submit"]{
    -moz-box-shadow: inset 0px 1px 0px 0px #45D6D6;
    -webkit-box-shadow: inset 0px 1px 0px 0px #45D6D6;
    box-shadow: inset 0px 1px 0px 0px #45D6D6;
    background-color: #2CBBBB;
    border: 1px solid #27A0A0;
    display: inline-block;
    cursor: pointer;
    color: #FFFFFF;
    font-family: 'Open Sans Condensed', sans-serif;
    font-size: 14px;
    padding: 8px 18px;
    text-decoration: none;
    text-transform: uppercase;
}
.form-style-8 input[type="button"]:hover,
.form-style-8 input[type="submit"]:hover {
    background:linear-gradient(to bottom, #34CACA 5%, #30C9C9 100%);
    background-color:#34CACA;
}
ul {
    list-style-type: none;
    margin: 0;
    padding: 0;
    overflow: hidden;
    background-color: #333;
}
li {
    float: left;
    border-right:0px solid #bbb;
}
    li:last-child {
    border-right: none;
}
li a {
    display: block;
    color: white;
    text-align: center;
    padding: 14px 16px;
    text-decoration: none;
}
li a:hover:not(.active) {
    background-color: #111;
}
html {
    height: 100%;
    box-sizing: border-box;
}

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
<div class="form-style-8">
<h1><p>Post a New Ride</p></h1>
<form name="form" method="post" action="">
<label>From City</label>
<select name = "source" class="styled-select slate">
<option value="">Select City</option>
<?php
    $query = "SELECT * FROM us_cities ORDER BY city_name";
    $results = mysqli_query($conn,$query);
    foreach($results as $city){
?>
<option value="<?php echo $city[city_name];?>"><?php echo $city[city_name];?></option>
<?php } ?>
</select>
<br>
<label>To City</label>
<select name = "destination" class="styled-select slate">
<option value="">Select City</option>
<?php
    $query = "SELECT * FROM us_cities ORDER BY city_name";
    $results = mysqli_query($conn,$query);
    foreach($results as $city){
?>
<option value="<?php echo $city[city_name];?>"><?php echo $city[city_name];?></option>
<?php } ?>
</select>
</br>
                           
<label>Car Make</label>
<select name = "make" class="styled-select slate">
<option value="">Select Car Make</option>
<?php
$query = "SELECT DISTINCT make FROM VehicleModelYear ORDER BY make";
$results = mysqli_query($conn,$query);
echo $results->num_rows;
foreach($results as $car){
?>
<option value="<?php echo $car[make];?>"><?php echo $car[make];?></option>
<?php } ?>
</select>
</br>
                           
<label>Car Model</label>
<select name = "model" class="styled-select slate">
<option value="">Select Car Model</option>
<?php
$query = "SELECT DISTINCT model FROM VehicleModelYear ORDER BY model";
$results = mysqli_query($conn,$query);
foreach($results as $car){
?>
<option value="<?php echo $car[model];?>"><?php echo $car[model];?></option>
<?php } ?>
</select>
</br>

<label>Cost</label>
<select name = "cost" class="styled-select slate">
<option value="select cost">Select cost</option>
<option value="15">15</option>
<option value="20">20</option>
<option value="25">25</option>
<option value="30">30</option>
<option value="35">35</option>
<option value="40">40</option>
<option value="45">45</option>
<option value="50">50</option>
<option value="55">55</option>
<option value="60">60</option>
<option value="65">65</option>
<option value="70">70</option>
<option value="75">75</option>
<option value="80">80</option>
<option value="85">85</option>
<option value="90">90</option>
option value="95">95</option>
<option value="100">100</option>
</select>
<br>
<label>Date of Travel</label>
<input type="date" name="dateFrom" value="<?php echo date('Y-m-d'); ?>">
</br>
<p><input name="submit" type="submit" value="Submit" /></p>
</form>
<p align="center" style="color:#FF0000;"><?php echo $status; ?></p>
</div>
</div>
</body>
</html>
                               

