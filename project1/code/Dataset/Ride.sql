-- phpMyAdmin SQL Dump
-- version 4.6.3
-- https://www.phpmyadmin.net/
--
-- Host: localhost:3306
-- Generation Time: Oct 12, 2016 at 12:56 AM
-- Server version: 10.1.17-MariaDB
-- PHP Version: 7.0.8

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `pickmeup`
--

-- --------------------------------------------------------

--
-- Table structure for table `Ride`
--

DROP TABLE IF EXISTS `Ride`;
CREATE TABLE `Ride` (
  `ride_id` int(10) UNSIGNED NOT NULL,
  `name_user` varchar(30) COLLATE utf8_unicode_ci NOT NULL,
  `source` varchar(30) COLLATE utf8_unicode_ci NOT NULL,
  `destination` varchar(30) COLLATE utf8_unicode_ci NOT NULL,
  `car_make` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
  `car_model` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
  `start_date` date DEFAULT NULL,
  `cost` int(10) NOT NULL,
  `booked` boolean NOT NULL default 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Dumping data for table `Ride`
--

INSERT INTO `Ride` (`ride_id`, `name_user`, `source`, `destination`, `car_make`, `car_model`, `start_date`, `cost`, `booked`) VALUES
(1, 'Satya', 'Ackley', 'Ackley', 'Ford', 'Mustang', '2018-10-06', 15, 0),
(2, 'Satya', 'Ackley', 'Ackworth', 'Ford', 'Mustang', '2018-9-26', 20, 0),
(3, 'Sardar', 'Ackley', 'Adair', 'Ford', 'Mustang', '2018-08-06', 25, 0),
(4, 'Sardar', 'Ackley', 'Adel', 'Ford', 'Mustang', '2018-05-13', 30, 0),
(5, 'Sardar', 'Ackley', 'Afton', 'Ford', 'Mustang', '2018-02-05', 35, 0),
(6, 'Sardar', 'Ackley', 'Agency', 'Ford', 'Mustang', '2018-06-06', 40, 0),
(7, 'Sardar', ' Adel', 'Ainsworth', 'Ford', 'Mustang', '2018-07-06', 45, 0),
(8, 'Sardar', ' Hazleton', 'Akron', 'Ford', 'Mustang', '2018-08-06', 50, 0),
(9, 'Sardar', ' Hazleton', 'Albert City', 'Ford', 'Mustang', '2018-09-06', 55, 0),
(10, 'Sardar', ' Hazleton', 'Albia', 'Ford', 'Mustang', '2018-05-06', 60, 0),
(11, 'Sardar', ' Hazleton', 'Albion', 'Ford', 'Mustang', '2018-04-06', 65, 0),
(12, 'Sardar', ' Hazleton', 'Alburnett', 'Ford', 'Mustang', '2018-03-06', 70, 0),
(13, 'Sardar', ' Hazleton', 'Alden', 'Ford', 'Mustang', '2018-05-06', 75, 0),
(14, 'Krishna', ' La Porte City', 'Alexander', 'Ford', 'Mustang', '2018-02-06', 80, 0),
(15, 'Krishna', ' Ames', 'Algona', 'Ford', 'Mustang', '2018-06-06', 85, 0),
(16, 'Krishna', 'Ackley', 'Alleman', 'Ford', 'Mustang', '2018-01-06', 90, 0),
(17, 'Krishna', 'Ackworth', 'Allerton', 'Ford', 'Mustang', '2018-05-06', 95, 0),
(18, 'Satya', 'Adair', 'Allison', 'Ford', 'Mustang', '2018-03-06', 100, 0),
(19, 'Satya', 'Adel', 'Alta', 'Ford', 'Mustang', '2018-07-06', 15, 0),
(20, 'Satya', 'Afton', 'Alta Vista', 'Ford', 'Mustang', '2018-07-06', 20, 0),
(21, 'Satya', 'Agency', 'Alton', 'Ford', 'Mustang', '2018-08-06', 25, 0),
(22, 'Satya', 'Ainsworth', 'Altoona', 'Ford', 'Mustang', '2018-02-06', 30, 0),
(23, 'Satya', 'Akron', 'Alvord', 'Ford', 'Mustang', '2018-07-06', 35, 0),
(24, 'Satya', 'Albert City', 'Amana', 'Ford', 'Mustang', '2018-09-07', 40, 0),
(25, 'Satya', 'Albia', 'Ames', 'Ford', 'Mustang', '2018-03-06', 45, 0),
(26, 'Satya', 'Albion', 'Anamosa', 'Ford', 'Mustang', '2018-06-23', 50, 1),
(27, 'Satya', 'Alburnett', 'Anderson', 'Ford', 'Mustang', '2018-06-06', 55, 1),
(28, 'Satya', 'Alden', 'Andover', 'Ford', 'Mustang', '2018-03-06', 60, 0),
(29, 'Satya', 'Alexander', 'Andrew', 'Ford', 'Mustang', '2018-09-06', 65, 0),
(30, 'Krishna', 'Algona', 'Anita', 'Ford', 'Mustang', '2018-10-06', 70, 0);

--
-- Indexes for dumped tables
--

--
-- Indexes for table `Ride`
--
ALTER TABLE `Ride`
  ADD PRIMARY KEY (`ride_id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `Ride`
--
ALTER TABLE `Ride`
  MODIFY `ride_id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=31;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
