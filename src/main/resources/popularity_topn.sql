/*
Navicat MySQL Data Transfer

Source Server         : 192.168.116.166
Source Server Version : 50505
Source Host           : 192.168.116.166:3306
Source Database       : test

Target Server Type    : MYSQL
Target Server Version : 50505
File Encoding         : 65001

Date: 2016-11-03 16:39:48
*/


SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for popularity_topn
-- ----------------------------
DROP TABLE IF EXISTS `popularity_topn`;
CREATE TABLE `popularity_topn` (
  `page_name` varchar(255) NOT NULL,
  `popularity` double(255,0) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
