--COVID-19 Data Exploration in SQL from 2020 to present

Select * From [PortfolioProjects-DataCleaning]..CovidDeaths$
Where continent is not null 
order by 3,4

Select Location, date, total_cases, new_cases, total_deaths, population From [PortfolioProjects-DataCleaning]..CovidDeaths$
Where continent is not null 
order by 1,2


-- Total Cases vs Total Deaths

Select Location, date, total_cases,total_deaths From [PortfolioProjects-DataCleaning]..CovidDeaths$
Where location = 'India' AND continent is not null 
order by 1,2

--Calculate the death percentage for India 

Select Location, date, total_cases,total_deaths, (cast(total_deaths AS float) / CAST(total_cases AS float))*100 AS DeathPercentage From [PortfolioProjects-DataCleaning]..CovidDeaths$
Where location = 'India' AND continent is not null 
order by 1,2


--Calculate the death percentage for India and United States

Select Location, date, total_cases,total_deaths, (cast(total_deaths AS float) / CAST(total_cases AS float))*100 AS DeathPercentage From [PortfolioProjects-DataCleaning]..CovidDeaths$
Where location = 'India' OR location = 'United States' AND continent is not null 
order by 1,2


-- Total Cases vs Population
-- Shows what percentage of population infected with Covid

Select Location, date, Population, total_cases, (total_cases/population)*100 as PercentPopulationInfected
From [PortfolioProjects-DataCleaning]..CovidDeaths$
Where location = 'United States'
order by 1,2


-- Countries with Highest Infection Rate compared to Population

Select Location, Population, MAX(total_cases) as HighestInfectionCount From [PortfolioProjects-DataCleaning]..CovidDeaths$
Group by Location,population
Order by HighestInfectionCount desc;

-- Countries with Highest Infection Rate compared to Population in the year 2020

Select Location, Population, MAX(cast(total_cases as float)) as HighestInfectionCount,  Max(total_cases/population)*100 as PercentPopulationInfected
From [PortfolioProjects-DataCleaning]..CovidDeaths$
Where YEAR(Date)=2020
Group by Location, Population
order by PercentPopulationInfected desc;

-- Countries with Highest Death Count and DeathRate per Population

Select Location,population, MAX(cast(Total_deaths as int)) as TotalDeathCount, Max(total_cases/population)*100 as DeathRate  From [PortfolioProjects-DataCleaning]..CovidDeaths$
--Where location like '%states%'
Where continent is not null 
Group by Location,population
order by DeathRate desc;

-- Contintents with the highest death count per population

Select continent, MAX(cast(Total_deaths as int)) as TotalDeathCount From [PortfolioProjects-DataCleaning]..CovidDeaths$
--Where location like '%states%'
Where continent is not null 
Group by continent
order by TotalDeathCount desc;

-- Total number of cases and deaths in the world

Select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
From [PortfolioProjects-DataCleaning]..CovidDeaths$
--Where location like '%states%'
where continent is not null 
--Group By date
order by 1,2

-- Total Population vs Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine

Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From [PortfolioProjects-DataCleaning]..CovidDeaths$ dea
Join [PortfolioProjects-DataCleaning]..CovidVaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
order by 2,3

--Total Vaccinations administered in each location and continent
Select dea.continent,dea.location,dea.population,dea.date,vac.total_vaccinations
From [PortfolioProjects-DataCleaning]..CovidDeaths$ dea
Join [PortfolioProjects-DataCleaning]..CovidVaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
order by 2,3;


--Total Rolling Vaccinations administered in each location and continent
Select dea.continent,dea.location,dea.population,dea.date,vac.new_vaccinations, 
Sum(cast(vac.new_vaccinations as float)) Over (Partition by dea.location ORDER By dea.date,dea.location) as RollingVaccinations
From [PortfolioProjects-DataCleaning]..CovidDeaths$ dea
Join [PortfolioProjects-DataCleaning]..CovidVaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
order by 2,3;

-- We want to find the percentage of people vaccinated on a daily basis when compared to the population of the location. 
-- But, since we cannot use the newly created "RollingVaccinations" column in the query we have to use a temp table or CTE to store the calculation.


-- Using CTE to perform Calculation on Partition By in previous query

With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingVaccinations)
as
(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, Sum(cast(vac.new_vaccinations as float))
OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingVaccinationPercentage
From [PortfolioProjects-DataCleaning]..CovidDeaths$ dea
Join [PortfolioProjects-DataCleaning]..CovidVaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
--order by 2,3
)
Select *,(RollingVaccinations/Population)*100 as RollingVaccinationPercentage
From PopvsVac


-- Using Temp Table to perform Calculation on Partition By in previous query

DROP Table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingPeopleVaccinated numeric
)

Insert into #PercentPopulationVaccinated
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From [PortfolioProjects-DataCleaning]..CovidDeaths$ dea
Join [PortfolioProjects-DataCleaning]..CovidVaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
--where dea.continent is not null 
--order by 2,3

Select *, (RollingPeopleVaccinated/Population)*100
From #PercentPopulationVaccinated


-- Creating View to store data for later visualizations

Create View PercentPopulationVaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From [PortfolioProjects-DataCleaning]..CovidDeaths$ dea
Join [PortfolioProjects-DataCleaning]..CovidVaccinations$ vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 






