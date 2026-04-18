-- EDA

-- Here we are just going to explore the data and find trends or patterns 


SELECT * 
FROM layoffs_staging;


SELECT MAX(total_laid_off)
FROM layoffs_staging;



-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_staging
WHERE  percentage_laid_off IS NOT NULL;


-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM layoffs_staging
WHERE  percentage_laid_off = 1;
-- these are mostly startups it looks like who all went out of business during this time


-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM layoffs_staging
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;







-- Companies with the biggest single Layoff
SELECT company, total_laid_off
FROM layoffs_staging
ORDER BY 2 DESC
LIMIT 5;


-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;



-- by location
SELECT location, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- by country
SELECT country, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY country
ORDER BY 2 DESC;

-- by date
SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_staging
GROUP BY YEAR(date)
ORDER BY 1 ASC;

-- by industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY industry
ORDER BY 2 DESC;

-- by stage
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY stage
ORDER BY 2 DESC;





-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. 

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging
  GROUP BY company, YEAR(date)
),
Company_Year_Rank AS
(
  SELECT company, years, total_laid_off, 
  DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;





-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
where SUBSTRING(date,1,7) is not NUll
GROUP BY dates
ORDER BY dates ASC;


-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS laid_off
FROM layoffs_staging
where SUBSTRING(date,1,7) is not NUll
GROUP BY dates
)
SELECT dates, 
SUM(laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE;



















































