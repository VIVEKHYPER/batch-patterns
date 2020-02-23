--
-- Database: `samplevideo_db`
--

-- --------------------------------------------------------

--
-- Table structure for table `user_details`
--
DROP TABLE IF EXISTS user_details;

CREATE TABLE IF NOT EXISTS user_details (
  user_id int(11) NOT NULL AUTO_INCREMENT,
  username varchar(255) DEFAULT NULL,
  first_name varchar(50) DEFAULT NULL,
  last_name varchar(50) DEFAULT NULL,
  gender varchar(10) DEFAULT NULL,
  password varchar(50) DEFAULT NULL,
  status tinyint(10) DEFAULT NULL,
  PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS user_details_temp (
  user_id int(11) NOT NULL AUTO_INCREMENT,
  first_name varchar(50) DEFAULT NULL,
  last_name varchar(50) DEFAULT NULL,
  PRIMARY KEY (user_id)
);

--

