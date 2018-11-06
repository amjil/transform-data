/*
Navicat SQLite Data Transfer

Source Server         : sqlite
Source Server Version : 30714
Source Host           : :0

Target Server Type    : SQLite
Target Server Version : 30714
File Encoding         : 65001

Date: 2018-11-05 11:49:09
*/

PRAGMA foreign_keys = OFF;

-- ----------------------------
-- Table structure for job_logs
-- ----------------------------
DROP TABLE IF EXISTS "main"."job_logs";
CREATE TABLE job_logs (id integer primary key autoincrement, batch_id text, job_date date, job_nm text);
