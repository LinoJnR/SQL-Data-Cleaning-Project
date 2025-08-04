#Data Cleaning

select*
From layoffs
;

#1.Remove Duplicates 
#2.Standadize the data
#3.Null Values or Blank values
#4.Remove any columnns or rows
#5.alter

create table layoffs_staging
like layoffs;

select*
From layoffs_staging
;

insert layoffs_staging
select*
from layoffs
;

select*,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,'date') as row_num
From layoffs_staging
;

With duplicates_cte as
(select*,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage
,country,funds_raised_millions) as row_num
From layoffs_staging
)
select*
from duplicates_cte
where row_num >1;

select*
From layoffs_staging2
where company = 'Casper'
;


With duplicates_cte as
(select*,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage
,country,funds_raised_millions) as row_num
From layoffs_staging
)
Delete
from duplicates_cte
where row_num >1;


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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select*
From layoffs_staging2
where row_num > 1
;

insert into layoffs_staging2
select*,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage
,country,funds_raised_millions) as row_num
From layoffs_staging
;

select*
From layoffs_staging2
where row_num = 1 ;

Delete
From layoffs_staging2
where row_num > 1 ;

select*
from layoffs_staging2
;

#standadizing data
 select company, trim(company)
 from layoffs_staging2
 ;
 
 update layoffs_staging2
 set company = trim(company)
 ;

select distinct industry
 from layoffs_staging2
 order by country 
 ;
 
 select industry, trim(industry)
 from layoffs_staging2
 order by 1
 ;
 
 update layoffs_staging2
 set industry = trim(industry)
 ;
 
 select*
 from layoffs_staging2
 where industry like 'crypto%';
 
 update layoffs_staging2
 set industry = 'crypto'
 where industry like 'crypto%';
 ;
 
 select distinct country
 from layoffs_staging2
 order by 1
 ;
 
  update layoffs_staging2
 set country = 'United States'
 where country like 'United States%';
 ;
 
 #or 
 select distinct country, trim(Trailing  '. ' from country)
 from layoffs_staging2
 order by 1
 ;
 ##fixing Date from text to date 
 select `date`
 from layoffs_staging2
 ;
 
 #checking the formal if it works
 select `date`,
 str_to_date(`date`,'%m/%d/%Y')
 from layoffs_staging2;
 
 #we use the str to date to update this field
 UPDATE layoffs_staging2
 SET  `date` = str_to_date(`date`,'%m/%d/%Y')
 ;
 #now we convert the data type properly
 alter table layoffs_staging2
 modify column `date` date;
 
 
 ####
 
  select*
From layoffs_staging
where total_laid_off is null 
and percentage_laid_off is null
;

update layoffs_staging2
set industry = null
where industry = '';

select* 
from layoffs_staging2
where industry is null 
or industry = '';
 
 select*
 from layoffs_staging2
 where company like 'Bally%';
 
 select t1.industry,t2.industry
 from layoffs_staging2 t1
 join layoffs_staging2 t2
	on t1.company = t2.company
where(t1.industry is null or t1.industry ='')
and t2.industry is not null
;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    set t1.industry = t2.industry
where (t1.industry is null )
and t2.industry is not null
;

select*
from layoffs_staging2
;
 select*
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null;
 
delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is  null;

select*
from layoffs_staging2;

alter table layoffs_staging2
drop column  row_num;
 