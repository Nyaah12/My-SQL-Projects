# SQL Project - Data Cleaning
# https://www.kaggle.com/datasets/swaptr/layoffs-2022


SELECT *
FROM layoffs;

#When we are data cleaning we usually follow a few steps
#1. Remove Data
#2. Standardize Data
#3. Null Values or Blank values
#4. Remove Any Columns or Rows

#First thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE layoffs_staging
LIKE layoffs;

#1 Remove Duplicates

# First let's check for duplicates

INSERT layoffs_staging
SELECT *
FROM layoffs;


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

#let's just look at Ada to confirm
SELECT *
FROM layoffs_staging
WHERE company = 'Ada';

#it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

#these are our real duplicates 
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

#these are the ones we want to delete where the row number is > 1 or 2or greater essentially

 #now you may want to write it like this:



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;


#2.Standardizing data


SELECT company, TRIM(company)
FROM  layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


SELECT *
FROM  layoffs_staging2
WHERE industry LIKE 'Crypto%';


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE'Crypto%';


SELECT *
FROM  layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

#everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

#Let's also fix the date columns:
SELECT *
FROM  layoffs_staging2;

SELECT `date`
FROM layoffs_staging2;

#we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

#now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;date;

#3. Look at Null Values

#the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
#I like having them null because it makes it easier for calculations during the EDA phase

# so there isn't anything I want to change with the null values

SELECT *
FROM  layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
     ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL; 


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
     ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;  

#4. remove any columns and rows we need to
SELECT *
FROM layoffs_staging2;


SELECT *
FROM  layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

#Delete Useless data we can't really use
DELETE
FROM  layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;





