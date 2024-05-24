"""module contains ddl query strings"""

testing_camps_and_clinics="""
CREATE TABLE `testing_Camps_and_Clinics` (
`id` INT NOT NULL AUTO_INCREMENT,
`event` VARCHAR(255) DEFAULT NULL,
  `eventDate` VARCHAR(90) DEFAULT NULL,
  `state` VARCHAR(90) DEFAULT NULL,
  `year` VARCHAR(90) DEFAULT NULL,
  `link` VARCHAR(255) DEFAULT NULL,
  `webSource` VARCHAR(90) DEFAULT NULL,
  `gender` VARCHAR(90) DEFAULT NULL,
  `added` VARCHAR(90) DEFAULT NULL,
  `division` VARCHAR(90) DEFAULT NULL,
PRIMARY KEY (`id`),
 UNIQUE KEY `UNIQUE_INDEX2` (  `event`, `state`, `year`, `gender`, `added`, `division`) 
)
"""

testing_Club_Rankings="""
CREATE TABLE `testing_Club_Rankings` (
`id` INT NOT NULL AUTO_INCREMENT,
`rank` DOUBLE DEFAULT NULL,
  `club` VARCHAR(90) DEFAULT NULL,
  `rating` DOUBLE DEFAULT NULL,
  `link` DOUBLE DEFAULT NULL,
  `gender` VARCHAR(90) DEFAULT NULL,
  `class` INT DEFAULT NULL,
  `season` INT DEFAULT NULL,
  `webSource` VARCHAR(90) DEFAULT NULL,
PRIMARY KEY (`id`),
 UNIQUE KEY `UNIQUE_INDEX2` (  `club`, `rating`, `link`, `gender`, `class`, `season`, `webSource`) 
)
"""

testing_College_Ranking="""
CREATE TABLE `testing_College_Rankings` (
`id` INT NOT NULL AUTO_INCREMENT,
`rank` INT DEFAULT NULL,
  `team` VARCHAR(90) DEFAULT NULL,
  `powerRating` INT DEFAULT NULL,
  `strengthOfSchedulePR` INT DEFAULT NULL,
  `qualityWinFactorPR` INT DEFAULT NULL,
  `championshipPercentage` DOUBLE DEFAULT NULL,
  `ratingPercentageIndex` INT DEFAULT NULL,
  `strengthOfScheduleRPI` INT DEFAULT NULL,
  `qualityWinFactorRPI` INT DEFAULT NULL,
  `selection` INT DEFAULT NULL,
  `gender` VARCHAR(90) DEFAULT NULL,
  `division` VARCHAR(90) DEFAULT NULL,
  `webSource` VARCHAR(90) DEFAULT NULL,
  `winRatio` VARCHAR(90) DEFAULT NULL,
PRIMARY KEY (`id`)
)
"""

testing_school_rankings="""
CREATE TABLE `testing_School_Rankings` (
`id` INT NOT NULL AUTO_INCREMENT,
`rank` INT DEFAULT NULL,
  `team` VARCHAR(90) DEFAULT NULL,
  `powerRating` INT DEFAULT NULL,
  `strengthOfSchedulePR` INT DEFAULT NULL,
  `qualityWinFactorPR` INT DEFAULT NULL,
  `championshipPercentage` DOUBLE DEFAULT NULL,
  `ratingPercentageIndex` DOUBLE DEFAULT NULL,
  `strengthOfScheduleRPI` DOUBLE DEFAULT NULL,
  `qualityWinFactorRPI` DOUBLE DEFAULT NULL,
  `selection` DOUBLE DEFAULT NULL,
  `gender` VARCHAR(90) DEFAULT NULL,
  `webSource` VARCHAR(90) DEFAULT NULL,
  `winRatio` VARCHAR(90) DEFAULT NULL,
PRIMARY KEY (`id`)
)
"""

Commitments_Summary="""
CREATE TABLE `Commitments_Summary` (
  `id` int NOT NULL AUTO_INCREMENT,
  `club` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `gender` varchar(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `commitmentYear` int DEFAULT NULL,
  `state` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `division` varchar(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `commitmentCount` bigint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
)
"""