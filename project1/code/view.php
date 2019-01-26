<?php
    require_once "conndb.php";
    session_start();
    $status = "";
    if(isset($_POST['radio']))
    {
        $id = $_REQUEST['radio'];
        $username = $_SESSION["username"];
        $ins_query="INSERT INTO book_user (book_id,user_name) VALUES ('$id','$username')";
        $upd_query="UPDATE Ride SET booked = 1 WHERE ride_id=$id";
        
        $check_query1="SELECT * FROM book_user WHERE book_id=$id and user_name='".$username."'";
        $check_res1 = mysqli_query($conn, $check_query1);
        
        $check_query2="SELECT name_user FROM Ride WHERE ride_id=$id";
        $check_res2 = mysqli_query($conn, $check_query2);
        
        $check_flag = 0;
        
        if ($check_res1->num_rows>0){
            $check_flag = 1;
        }
        
        if($check_res2){
            foreach($check_res2 as $val){
                if($val[name_user] == $username){
                    $check_flag = 2;
                }
            }

            if ($check_flag != 0){
                if ($check_flag == 1){
                    $status = "Already booked.
                    <br></br><a href='view.php'>View Available Rides</a>";
                }else{
                    $status = "Please check again.
                    <br></br><a href='view.php'>View Available Rides</a>";
                }
            }else{
                if(mysqli_query($conn,$ins_query))
                {
                    mysqli_query($conn, $upd_query);
                    $status = "Booked ride Successfully.
                    <br></br><a href='history.php'>View Ride History</a>";
                }
            }
            
        }
    }
    
?>

<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>View Records</title>
<link rel="stylesheet" href="css/style.css" />
<style>
input[type=submit] {
    background-color: #008CBA;
    border: none;
    border-radius: 10px;
    color: white;
    padding: 10px 22px;
    text-decoration: none;
    margin: 4px 2px;
    cursor: pointer;
}
</style>
</head>
<body>
<div class="container">
<ul>
<li><a href="home.php">PickMeUp</a></li>
<li><a href="home.php">Home</a></li>
<li><a href="view.php">Book a Ride</a></li>
<li><a href="history.php">My Booked Ride</a></li>
<li><a href="post.php">Post a Ride</a></li>
<li><a href="cancelpost.php">My Posted Ride</a></li>
<li style="float:right"><a href="logout.php">Logout</a></li>
</ul>
<h1><p align="center">Book a Ride</p></h1>
<br>
<table align="center" width="100%" border="1" style="border-collapse:collapse;", name="table">
<thead>
<tr>
<th><strong>Ride Id</strong></th>
<th><strong>Posted By</strong></th>
<th><strong>From City</strong></th>
<th><strong>To City</strong></th>
<th><strong>Car Make</strong></th>
<th><strong>Car Model</strong></th>
<th><strong>Cost</strong></th>
<th><strong>Travel Date</strong></th>
<th><strong>Select</strong></th>
</tr>
</thead>

<?php
    
    $pageSize = 5;
    $rowCount = 0;
    $pageNow = 1;
    
    if(!empty($_GET['pageNow'])){
        $pageNow = $_GET['pageNow'];
    }
    
    //$pageCount = 0;
    $query1 = "SELECT * FROM Ride WHERE booked=0";
    $res1 = mysqli_query($conn,$query1);
    if($row1 = mysqli_fetch_assoc($res1)){
        $rowCount = $res1->num_rows;
    }
    
    $pageCount = ceil($rowCount/$pageSize);
    
    $pre = ($pageNow - 1)*$pageSize;
    
    
    $sel_query="SELECT * FROM Ride WHERE booked = 0 ORDER BY ride_id DESC LIMIT $pre, $pageSize";
    $result = mysqli_query($conn,$sel_query);
    while($row = mysqli_fetch_assoc($result)) { ?>
    <tr>
        <td align="center"><?php echo $row["ride_id"]; ?></td>
        <td align="center"><?php echo $row["name_user"]; ?></td>
        <td align="center"><?php echo $row["source"]; ?></td>
        <td align="center"><?php echo $row["destination"]; ?></td>
        <td align="center"><?php echo $row["car_make"]; ?></td>
        <td align="center"><?php echo $row["car_model"]; ?></td>
        <td align="center"><?php echo $row["cost"]; ?></td>
        <td align="center"><?php echo $row["start_date"]; ?></td>
        <form name="form" method="post" action="">
        <td align="center"><label><input type="radio" name="radio" value="<?php echo $row["ride_id"]; ?>"></label></td>
    </tr>

<?php } ?>
</table> 
<?php
    
    if (($pageNow>1) && ($pageNow<$pageCount)){
        $prePage = $pageNow - 1;
        $nextPage = $pageNow + 1;
        echo "<p align='center'><a href='view.php?pageNow=$prePage'>Previous </a>";
        echo "<a href='view.php?pageNow=$nextPage'>Next </a>";
        echo "{$pageNow}/{$pageCount}</p>";
    }
    
    if ($pageNow == 1){
        $nextPage = $pageNow + 1;
        echo "<p align='center'><a href='view.php?pageNow=$nextPage'>Next </a>";
        echo "{$pageNow}/{$pageCount}</p>";
    }
    
    if ($pageNow==$pageCount){
        $prePage = $pageNow - 1;
        echo "<p align='center'><a href='view.php?pageNow=$prePage'>Previous </a>";
        echo "{$pageNow}/{$pageCount}</p>";
    }
?>

<p align = "center"><input name="submit" type="submit" value="Confirm" /></p>
</form>
<p style="color:#FF0000;"><?php echo "<p align='center'>{$status}</p>"; ?></p>
</div>

</body>
</html>
