-- remove duplicate
-- standardize data
-- null values or blank values
-- remove any column

select * from layoffs;


-- create duplicate table for cleaning
create table layoffs_staging 
like layoffs;  -- add only cloumn 

insert into layoffs_staging
select * from layoffs;  -- add data from table




-- 1. remove duplicate

-- assign unique no. to identify duplicate by windows function. if unique ID is not in table
with r_duplicate as (
	select *,
    row_number() over(partition by  company,
        location,
        industry,
        COALESCE(total_laid_off, 0),
        COALESCE(percentage_laid_off, 0),
        date,
        stage,
        country,
        COALESCE(funds_raised_millions, 0)) rn_num
        from layoffs_staging
)
select * from r_duplicate
where rn_num > 1; 



-- delete duplicate 
with r_duplicate as (
	select *,
    row_number() 
    over(partition by  company,
        location,
        industry,
        COALESCE(total_laid_off, 0),
        COALESCE(percentage_laid_off, 0),
        date,
        stage,
        country,
        COALESCE(funds_raised_millions, 0)) rn_num
        from layoffs_staging
)
delete from layoffs_staging
where ( company, location, industry, COALESCE(total_laid_off, 0), COALESCE(percentage_laid_off, 0), date,
        stage, country, COALESCE(funds_raised_millions, 0)) 
        IN
        (select company, location, industry, COALESCE(total_laid_off, 0), COALESCE(percentage_laid_off, 0), date,
        stage, country, COALESCE(funds_raised_millions, 0)
        from r_duplicate
        where rn_num > 1
        ); 
        




-- 2. standardize data (finding issue in data and soliving it)

-- COALESCE (column,replace with) handle NULL replace with anthing mention 
-- REPLACE(col, '.', '') to remove dots from anywhere in the word
-- trim(trailing '.' from country) remove from end only anything mention
-- trim(leading '.' from country) remove from first only anything mention
-- trim(both '.' from country) remove from both side anything mention
-- text to date / str_to_date(date, '%m/%d/%Y')

select company, trim(company)
from layoffs_staging
order by 1 ;

update layoffs_staging
set company = trim(company); -- remove extra spaces

select industry
from layoffs_staging
where industry like 'crypto%';

update layoffs_staging
set industry = 'crypto'
where industry like 'crypto%'; 

select distinct country
from layoffs_staging
order by 1 ;

update layoffs_staging
set country = trim(trailing '.' from country) 
where country like 'United States%';

select date,
str_to_date(date, '%m/%d/%Y')
from layoffS_staging;

update layoffs_staging
set date = str_to_date(date, '%m/%d/%Y'); 

alter table layoffs_staging
modify column date Date; -- change data-type text to date

alter table layoffs_staging
modify column percentage_laid_off decimal(5,2); 





-- 3. remove or replace null or blank

select * from layoffs_staging
where industry is NUll
or industry = '';


-- try to fill null and blank with same data for same company name or location rather then delete for indutry column only
select *
from layoffs_staging t1
join layoffs_staging t2
 on t1.company = t2.company
 and t1.location = t2.location
where (t1.industry is Null or t1.industry = '')
and t2.industry is not NUll;

update layoffs_staging
set industry = Null
where industry = ''; -- fill blanks with null



-- fill same company name or location with same industry value
update layoffs_staging t1
join layoffs_staging t2
 on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is Null or t1.industry = '')
and t2.industry is not NUll;





-- 4. remove rows or column

select *
from layoffs_staging
where total_laid_off is Null
and percentage_laid_off is Null;

delete from layoffs_staging
where total_laid_off is Null
and percentage_laid_off is Null;

select * from layoffs_staging;










 







 


