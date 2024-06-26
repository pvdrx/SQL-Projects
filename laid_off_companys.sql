select * from layoffs_staging;

select *,
row_number()over(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num /* criando coluna para remoção de dados duplicados */
from layoffs_staging;

WITH duplicate_cte as
(
SELECT *,
row_number()over(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
SELECT * from duplicate_cte
WHERE row_num >1;

CREATE TABLE `layoffs_staging2` ( /* criando tabela que sera alterada */
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

INSERT INTO layoffs_staging2  /* alimentando table que sera alterada com dados  */
SELECT *,
row_number()over(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging;

DELETE FROM layoffs_staging2 WHERE row_num >1; /* removendo duplicadas */

UPDATE layoffs_staging2
SET company = TRIM(company);  /* Limpando valores com espaço extra  */

SELECT industry
from layoffs_staging2;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; /* removendo variações da mesma industria */

UPDATE layoffs_staging2
SET location = 'Florianopolis'
WHERE location LIKE 'Florian%'; /* removendo entradas com valores quebrados*/

UPDATE layoffs_staging2
SET location = 'Dusseldorf'
WHERE location = 'DÃ¼sseldorf'; /* removendo entradas com valores quebrados*/

UPDATE layoffs_staging2
SET location = 'Malmo'
WHERE location = 'MalmÃ¶';      /* removendo entradas com valores quebrados*/

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country = 'United States.';  /* removendo entradas com valores duplicados */

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y'); /* transformando texto em formato data */

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; /* convertendo o texto ja padronizado em data para DATE na table */

UPDATE layoffs_staging2
SET industry = NULL      /* removendo valores " " */
WHERE industry = ''; 

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2			/* mostrando valores não tenham nada na coluna industry */
	ON t1.company = t2.company
WHERE (t1.industry IS NULL);

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company		/* adicionando valores que faltavam na coluna industry */
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

SELECT * FROM layoffs_staging2;

SELECT DISTINCT industry from layoffs_staging2 order by 1;

DELETE FROM layoffs_staging2		/* removendo valores com dados impossiveis de recuperar */
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num; 
