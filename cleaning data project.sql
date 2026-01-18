SELECT *
FROM layoffs;
 
CREATE TABLE layoffs_taging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_taging
SELECT *
FROM layoffs;

## removing duplicates
SELECT *
FROM layoffs_taging;

SELECT *,
row_number() OVER( 
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
AS 'row_num'
FROM layoffs_taging;

WITH duplicate_CTE AS
(
SELECT *,
row_number() OVER( 
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
AS 'row_num'
FROM layoffs_taging
)
SELECT *
FROM duplicate_CTE
WHERE row_num > 1;

SELECT *
FROM layoffs_taging
WHERE row_num >1;


CREATE TABLE `layoffs_taging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_taging2
WHERE row_num > 1;

INSERT INTO layoffs_taging2
SELECT *,
row_number() OVER( 
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
AS 'row_num'
FROM layoffs_taging;

DELETE
FROM layoffs_taging2
WHERE row_num > 1;

SELECT *
FROM layoffs_taging2;

##standardizing data 

SELECT company, TRIM(company)
FROM layoffs_taging2;

UPDATE layoffs_taging2
SET  company= TRIM(company);

SELECT DISTINCT industry
FROM layoffs_taging2
ORDER BY 1;

SELECT *
FROM layoffs_taging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_taging2
SET industry= 'Crypto'
WHERE industry LIKE 'crypto%';

SELECT  DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_taging2
ORDER BY 1;

UPDATE layoffs_taging2
SET country =TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States';

SELECT `date`
FROM layoffs_taging2;

UPDATE layoffs_taging2
SET `date` = str_to_date(`date` , '%m/%d/%y');

ALTER TABLE layoffs_taging2
MODIFY COLUMN `date` DATE;

## Removing nulls

SELECT *
FROM layoffs_taging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_taging2
WHERE industry IS NULL
OR industry = '';

UPDATE layoffs_taging2
SET industry = NULL 
WHERE industry ='';

SELECT *
FROM layoffs_taging2 AS t1
JOIN layoffs_taging2 AS t2
ON company.t1 = company.t2
WHERE industry.t1 IS NULL 
AND industry.t2 IS NOT NULL;

UPDATE layoffs_taging2
SET industry.t2= industry.t1
WHERE industry.t1 IS NULL 
AND industry.t2 IS NOT NULL;

## DELETE COLUMNS

SELECT *
FROM layoffs_taging2;

ALTER TABLE layoffs_taging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_taging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_taging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;