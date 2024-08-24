# Exploratory Data Analysis
# Here I am going to explore the data and find trends or patterns or anything interesting like outliers

SELECT *
 FROM world_layoffs.layoffs_staging2;
 
 
 SELECT MAX(total_laid_off), MAX(percentage_laid_off)
 FROM world_layoffs.layoffs_staging2;
 
 # Looking at Percentage to see how big these layoffs were
SELECT *
 FROM world_layoffs.layoffs_staging2
 WHERE percentage_laid_off = 1
 ORDER BY funds_raised_millions DESC;
 
 #Companies with the most Total Layoffs
 SELECT company, SUM(total_laid_off)
 FROM world_layoffs.layoffs_staging2
 GROUP BY company
 ORDER BY 2 DESC;
 
 
 #looking at dates
SELECT MIN(`date`), MAX(`date`)
 FROM world_layoffs.layoffs_staging2;
 
 
  SELECT `date`, SUM(total_laid_off)
 FROM world_layoffs.layoffs_staging2
 GROUP BY `date`
 ORDER BY 1 DESC;
 
 #I'm now looking at total layoffs by  years
  SELECT YEAR(`date`), SUM(total_laid_off)
 FROM world_layoffs.layoffs_staging2
 GROUP BY YEAR(`date`)
 ORDER BY 1 DESC;
 
 
 #Industries with the most Total Layoffs
  SELECT industry, SUM(total_laid_off)
 FROM world_layoffs.layoffs_staging2
 GROUP BY industry
 ORDER BY 2 DESC;
 
 SELECT *
 FROM world_layoffs.layoffs_staging2;

 #countries with the most Total Layoffs
  SELECT country, SUM(total_laid_off)
 FROM world_layoffs.layoffs_staging2
 GROUP BY country
 ORDER BY 2 DESC;
 
 #stages with the most total layoffs
 SELECT stage, SUM(total_laid_off)
 FROM world_layoffs.layoffs_staging2
 GROUP BY stage
 ORDER BY 2 DESC;
 
 SELECT country, SUM(percentage_laid_off)
 FROM world_layoffs.layoffs_staging2
 GROUP BY country
 ORDER BY 2 DESC;


#Rolling Total of Layoffs Per Month
SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

#now use it in a CTE so we can query off of it
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off,
SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


SELECT company, SUM(total_laid_off)
 FROM world_layoffs.layoffs_staging2
 GROUP BY company
 ORDER BY 2 DESC;
 
 #Earlier I looked at Companies with the most Layoffs. Now I"ll look at that per year.
 
 SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM world_layoffs.layoffs_staging2
 GROUP BY company, YEAR(`date`)
 ORDER BY company ASC;

#I also want to rank which year the companies laid off the most employees
 SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM world_layoffs.layoffs_staging2
 GROUP BY company, YEAR(`date`)
 ORDER BY 3 DESC;
 
 
 WITH Company_Year (company,  years, total_laid_off)AS
 (
 SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM world_layoffs.layoffs_staging2
 GROUP BY company, YEAR(`date`)
 )
 SELECT *,
 DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
 FROM Company_Year
 WHERE years IS NOT NULL
 ORDER BY Ranking ASC;
 
 #I will also filter the ranking
 WITH Company_Year (company,  years, total_laid_off)AS
 (
 SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM world_layoffs.layoffs_staging2
 GROUP BY company, YEAR(`date`)
 ), Company_Year_Rank AS
 (SELECT *,
 DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
 FROM Company_Year
 WHERE years IS NOT NULL
 )
SELECT *
FROM  Company_Year_Rank
WHERE Ranking <= 5;