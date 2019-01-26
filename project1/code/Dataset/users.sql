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
-- Table structure for table `users`
--
DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `id` int(11) NOT NULL,
  `username` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
  `email` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
  `password` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
  `trn_date` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

--
-- Dumping data for table `users`
--

INSERT INTO `users` (`id`, `username`, `email`, `password`, `trn_date`) VALUES
(1, 'Satya', 'ss@gmail.com', '03c7c0ace395d80182db07ae2c30f034', '2016-10-11 00:18:11'),
(2, 'Sardar', 's@ss.com', '872cacff48ab5fd129c45d25bd472de0', '2016-10-11 02:38:39'),
(3, 'Krishna', 'k@ggg.com', '243bd1ce0387f18005abfc43b001646a', '2016-10-11 08:43:58'),
(4, 'Rahul', 'rah@gmail.com', '439ed537979d8e831561964dbbbd7413', '2016-10-11 10:47:52'),
(5, 'Rahul', 'rah@gmail.com', '439ed537979d8e831561964dbbbd7413', '2016-10-11 10:48:50'),
(6, 'Rahul', 'rah@gmail.com', '03c7c0ace395d80182db07ae2c30f034', '2016-10-11 10:50:14'),
(7, 'Stark', 'mail2saideepreddy@gmail.com', '202cb962ac59075b964b07152d234b70', '2016-10-11 10:56:38'),
(8, 'Kalyan', 'kalyan@123.com', '028ddefe12c1d819b78727ae42fdb9d1', '2016-10-11 15:56:25'),
(9, 'user1', 'user1@gmail.com', '24c9e15e52afc47c225b757e7bee1f9d', '2016-10-11 23:35:41');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `users`
--
ALTER TABLE `users`
  ADD PRIMARY KEY (`id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `users`
--
ALTER TABLE `users`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=10;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
