--COVID-19: Data Exploration
--This code was used to explore COVID-19 data using a variety of techniques, including:
--aggregate functions, data type conversion, joins, arithmetic functions, temporary tables, CTEs, window functions, and creating views.

 -- Let's view the dataset*
SELECT *
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 3,4


SELECT*
FROM PortfolioProject..CovidVaccinations
ORDER BY 3,4

-- Selecting the Data For Analysis*
SELECT location,date, total_cases, new_cases,total_deaths,population
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 3,4


-- By Continent Overview*
SELECT Continent,date, total_cases, new_cases,total_deaths,population
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 3,4

--Summary by Continent 
SELECT Continent, MAX(CONVERT(int,total_deaths)) AS DeathCount 
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY Continent


-- Total cases Vs.Total deaths per Location
-- Likelihood of dying if infected in each location recorded on the data*

SELECT 
    location, date,total_cases,total_deaths, 
    (CAST(total_deaths AS float)/CAST(total_cases AS float)) * 100 AS Deaths
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY location, date



--*Comparison of Total Cases, Deaths, and Death Percentage 
--between Asia and North America by Year in Covid-19 Dataset*
SELECT Continent, YEAR(date) AS Year, 
       SUM(CAST(total_cases AS float)) AS TotalCases, 
       SUM(CAST(total_deaths AS float)) AS TotalDeaths, 
       (SUM(CAST(total_deaths AS float)) / NULLIF(SUM(CAST(total_cases AS float)), 0)) * 100 AS DeathsInPercentage
FROM PortfolioProject..CovidDeaths
WHERE continent IN ('Asia', 'North America') AND continent IS NOT NULL
GROUP BY Continent, YEAR(date)
ORDER BY 1, 2

-- Let's see how USA is doing and it's likelihood of being a pandemic causualty if infected.*
SELECT 
    location,date,total_cases,total_deaths, 
    (CAST(total_deaths AS float)/CAST(total_cases AS float)) * 100 AS DeathsInPercentage
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL 
    AND location = 'United States'
ORDER BY 3 DESC


-- Total Cases Vs Population*
SELECT location,date,population,total_cases, (total_cases/population)*100 AS InfectedPercentage
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
Order by 1,2


-- Country with the highest COVID-19 infections in their population*
SELECT location,population, MAX(total_cases)AS HigestInfectionCnt, MAX(total_cases/population)*100 AS InfectedPercentage
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location,population
ORDER BY 4 DESC



-- Infection rates By Continent For Each Country*
SELECT Continent,population, MAX(total_cases)AS HigestInfectionCnt, MAX(total_cases/population)*100 AS InfectedPercentage
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY Continent,population
ORDER BY 4 DESC


-- Location with the highest COVID-19 death count relative to their population*
SELECT location, MAX(CAST(total_deaths AS int)) AS DeathCount
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location,population
ORDER BY 2 DESC


-- Global Summary Overview: Ratio of new deaths to new cases in a given time period*
SELECT Date, SUM(new_cases)AS NewCasesGlobalCount,SUM(CAST(new_deaths AS int))AS DeathCountGlobal    
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY date 
ORDER BY Date DESC


-- Global Summary Overview: Death Rate Per day* 
SELECT Date, SUM(new_cases) AS NewCasesGlobalCount,SUM(CAST(new_deaths AS int)) AS DeathCountGlobal, 
    CASE 
        WHEN SUM(new_cases) = 0 
        THEN NULL 
        ELSE (SUM(CAST(new_deaths AS int))/SUM(new_cases))*100 
    END AS DeathCountPercentage
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY date 
ORDER BY 3 DESC


-- Global Summary Overview: Total Summary of death rate per new cases
SELECT SUM(new_cases)AS TotalGlobalNewCases,SUM(CAST(new_deaths AS int))AS GlobalDeathCount, 
(SUM(CAST(new_deaths AS int))/SUM(new_cases))*100 AS DeathCountPercentage
FROM PortfolioProject..CovidDeaths
--WHERE location like 'Canada'
WHERE continent IS NOT NULL 
SELECT*
FROM PortfolioProject..CovidVaccinations
ORDER BY 3,4
SELECT*
FROM PortfolioProject..CovidDeaths
ORDER BY 3,4


--Joining the Covid Deaths and Covid Vaccination Table*
SELECT *
FROM PortfolioProject..CovidDeaths AS death
JOIN PortfolioProject..CovidVaccinations As Vax
	ON death.location =Vax.location
	AND death.date = Vax.date


--PARTITION BY location*


SELECT death.continent, death.location, death.date, death.population, vax.new_vaccinations,
SUM(CAST(vax.new_vaccinations AS BIGINT)) OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS NewVaccinationSum  
FROM PortfolioProject..CovidDeaths AS death
JOIN PortfolioProject..CovidVaccinations As Vax
	ON death.location =Vax.location
	AND death.date = Vax.date
WHERE death.continent IS NOT NULL
ORDER BY 2,3


-- Using CTE for calculation with PARTITION BY*
WITH VaxPop (Continent, location, Date, Population, new_vaccinations, NewVaccinationSum)AS 
(
SELECT death.continent, death.location,death.date,death.population, vax.new_vaccinations,
SUM(CAST(vax.new_vaccinations AS INT))OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS NewVaccinationSum  
FROM PortfolioProject..CovidDeaths AS death
JOIN PortfolioProject..CovidVaccinations As Vax
	ON death.location =Vax.location
	AND death.date = Vax.date
WHERE death.continent IS NOT NULL

--ORDER BY 2,3
)
SELECT *, (NewVaccinationSum/Population)*100 AS PercenatagePopVaxxed
FROM VaxPop

-- Using Temp Table to Perform calculation on Partition By in Previous Query
DROP TABLE IF EXISTS #VaccPop
CREATE TABLE #VaccPop
(Continent varchar(50),
Location Varchar (50),
date datetime,
Population int,
new_vaccination int,
NewVaccinationSum int)
INSERT INTO #VaccPop
SELECT death.continent, death.location,death.date,death.population, vax.new_vaccinations,
SUM(CAST(vax.new_vaccinations AS INT))OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS NewVaccinationSum  
FROM PortfolioProject..CovidDeaths AS death
JOIN PortfolioProject..CovidVaccinations As Vax
	ON death.location =Vax.location
	AND death.date = Vax.date
	WHERE death.continent IS NOT NULL
-- Order By 2,3
SELECT*
FROM  #VaccPop
Order By 1,2

-- Creating View to store data for later Visualization 
CREATE VIEW VaccPop AS
SELECT death.continent, death.location,death.date,death.population, vax.new_vaccinations,
SUM(CAST(vax.new_vaccinations AS INT))OVER (PARTITION BY death.location ORDER BY death.location, death.date) AS NewVaccinationSum  
FROM PortfolioProject..CovidDeaths AS death
JOIN PortfolioProject..CovidVaccinations As Vax
	ON death.location =Vax.location
	AND death.date = Vax.date
WHERE death.continent IS NOT NULL