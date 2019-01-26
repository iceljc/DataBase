-- phpMyAdmin SQL Dump
-- version 4.6.3
-- https://www.phpmyadmin.net/
--
-- Host: localhost:3306
-- Generation Time: Oct 12, 2016 at 12:55 AM
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
-- Table structure for table `booking`
--
DROP TABLE IF EXISTS `book_user`;
CREATE TABLE `book_user` (
  `book_id` int(10) NOT NULL ,
  `user_name` varchar(32) COLLATE utf8_unicode_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Dumping data for table `book_user`
--

INSERT INTO `book_user` (`book_id`, `user_name`) VALUES
(1, 'Tom'),
(2, 'Jim'),
(3, 'Krishna'),
(4, 'Ryan'),
(5, 'Peter'),
(6, 'John'),
(7, 'Sardar'),
(8, 'Ben'),
(9, 'Frank'),
(10, 'Larry'),
(11, 'Brian'),
(12, 'Tim'),
(13, 'Satya'),
(14, 'Dede'),
(15, 'Grace'),
(16, 'Chris'),
(17, 'Chole');

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;


ALTER TABLE `book_user`
    ADD PRIMARY KEY (`book_id`);
