use layoff;
select * from layoffs;

-- 1. Remove duplicates
-- 2. Standardize data(correct spelling errors)
-- 3. Treating null values or blank values
-- 4. Remove any columns or rows


create table layoff_staging as select * from layoffs;
select * from layoff_staging;


-- 1. Removing dupicates

with duplicate_cte as (select *, row_number()over(partition by company, location, 
industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) 
as row_num from layoff_staging) select * from duplicate_cte where row_num>1;

select * from layoff_staging where company='Hibob';
create table layoff_staging2 as select *, row_number()over(partition by company, location, 
industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) 
as row_num from layoff_staging;
select * from layoff_staging2;
select * from layoff_staging2 where row_num>1;
delete from layoff_staging2 where row_num>1;


-- 2. Standardizing the data

-- Remove whitespaces
select * from layoff_staging2;
update layoff_staging2 set company=trim(company);

select distinct(industry) from layoff_staging2 order by 1;
select * from layoff_staging2 where industry like 'Crypto%';
update layoff_staging2 set industry='Crypto' where industry like 'Crypto%';

select distinct(location) from layoff_staging2 order by 1;
select distinct(country) from layoff_staging2 order by 1;
select * from layoff_staging2 where country like 'United States.';
update layoff_staging2 set country='United States' where country like 'United States%';
-- One more way for above country=trim(trailing '.' from country)

select date from layoff_staging2;
update layoff_staging2 set date=str_to_date(date,'%m/%d/%Y');
alter table layoff_staging2 modify column date date;
describe layoff_staging2;
select * from layoff_staging2;


-- 3. Treating null and blank values

select * from layoff_staging2 where total_laid_off is NULL and percentage_laid_off is NULL;
select * from layoff_staging2 where industry is NULL or industry='';
update layoff_staging2 set industry=NULL where industry='';
select t1.industry, t2.industry from layoff_staging2 as t1 join layoff_staging2 as t2 
on t1.company=t2.company where t1.industry is NULL and t2.industry is not NULL;
update layoff_staging2 as t1 join layoff_staging2 as t2 on t1.company=t2.company 
set t1.industry=t2.industry where t1.industry is NULL and t2.industry is not NULL;


-- 4. Removing columns or rows

create table layoff_staging3 as select * from layoff_staging2;
select * from layoff_staging3;
select * from layoff_staging3 where total_laid_off is null and percentage_laid_off is null;
delete from layoff_staging3 where total_laid_off is null and percentage_laid_off is null; 
select * from layoff_staging3;

alter table layoff_staging3 drop column row_num;
select * from layoff_staging3;



-- EXPLORATORY DATA ANALYSIS

-- 1. Maximum layoffs and maximum percentage of layoffs
select max(total_laid_off), max(percentage_laid_off) from layoff_staging3;

-- 2. Records where percentage of layoffs is 100
select * from layoff_staging3 where percentage_laid_off=1 order by total_laid_off desc;
select * from layoff_staging3 where percentage_laid_off=1 order by funds_raised_millions desc;

-- 3. Records where total layoffs is the maximum
select * from layoff_staging3 where total_laid_off=12000;

-- 4. Analysis of number of layoffs for each company, industry, country, date, year and stage
select company, sum(total_laid_off) from layoff_staging3 group by company order by 2 desc;
select industry, sum(total_laid_off) from layoff_staging3 group by industry order by 2 desc;
select country, sum(total_laid_off) from layoff_staging3 group by country order by 2 desc;
select date, sum(total_laid_off) from layoff_staging3 group by date order by 1 desc;
select Year(date), sum(total_laid_off) from layoff_staging3 group by Year(date) order by 1 desc;
select stage, sum(total_laid_off) from layoff_staging3 group by stage order by 2 desc;

-- 5. First and last date of layoff
select min(date), max(date) from layoff_staging3;

-- 6. Total layoffs for each Month
select substring(date,1,7) as month, sum(total_laid_off) from layoff_staging3 where substring(date,1,7) is not null
group by month order by 1 asc;

-- 7. Cumulative layoffs for each Month
with rolling_total as (select substring(date,1,7) as month, sum(total_laid_off) as total_off
from layoff_staging3 where substring(date,1,7) is not null
group by month order by 1 asc) select month, total_off, sum(total_off) over(order by month) as rolling_total
from rolling_total;

-- 8. Years with companies and total number of layoffs of each company where rank>=5
with ranking_cte as (select company, year(date), sum(total_laid_off) as total_off, 
dense_rank()over(partition by year(date) order by sum(total_laid_off) desc) as ranking from layoff_staging3 
where year(date) is not null group by company,year(date)) select * from ranking_cte where ranking<=5;

