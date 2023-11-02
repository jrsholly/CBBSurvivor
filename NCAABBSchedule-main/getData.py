import argparse
import pandas as pd 
import datetime
from time import sleep
from urllib import request
import ssl
import pytz
import json
import sys
import distutils.util

def getContext(date_num):
    url = f'https://www.espn.com/mens-college-basketball/schedule/_/date/{date_num}/group/50'
    context = ssl._create_unverified_context()
    response = request.urlopen(url, context=context)
    html = response.read()

    return html


def getKenpomRankings():
    kenpom_df = pd.read_excel(io="NCAABBSchedule-main/KenpomRankings.xlsx", sheet_name="Sheet1")
    updated_kenpom_df = standardizeTeamNames(kenpom_df)
    return updated_kenpom_df


def createScheduleDF(start_date, end_date):
    dfSchedule = pd.DataFrame()
    schedule_date = start_date
    while schedule_date <= end_date:
        htmlContext = getContext(schedule_date.strftime("%Y%m%d"))
        try:
            my_df = pd.read_html(htmlContext)[0]
            if len(my_df) > 0:
                my_df['Date'] = schedule_date.strftime("%Y-%m-%d")
                my_df.drop(columns=['TIME'],inplace=True)
                my_df.drop(columns=['TV'],inplace=True)
                my_df.drop(columns=['tickets'],inplace=True)
                my_df.drop(columns=['location'],inplace=True)
                dfSchedule = pd.concat([dfSchedule,my_df])
                print(f"Retrieved schedule data for {schedule_date}")
                schedule_date = schedule_date + datetime.timedelta(days=1)
                # sleep(1)
        except Exception as e:
            print(e)
            schedule_date = schedule_date + datetime.timedelta(days=1)
            # sleep(1)
    return dfSchedule

def getScheduleForDateRange(start_date, end_date):
    df = createScheduleDF(start_date, end_date)
    return df   

def cleanup(df):
    df = df.rename(columns={'MATCHUP':'Away','MATCHUP.1':'Home'})
    df['Home'] = df['Home'].apply(lambda x: removeAtSign(x))
    df['Away'] = df['Away'].apply(lambda x: removeRankings(x))
    df['Home'] = df['Home'].apply(lambda x: removeRankings(x))
    return df

def connor(x):
    return 'Connor'

def removeRankings(x):
    l = x.split(' ')
    if l[0].isdigit():
        del l[0]
        return ' '.join(l)
    return x

def removeAtSign(x):
    return x[2:]


def standardizeTeamNames(kenpom_rankings_df):
    f = open('NCAABBSchedule-main/teamNameDiffs.json')
    diff_dict = json.load(f)
    for team in diff_dict:
        updated = kenpom_rankings_df['Team'] == team['kp']
        kenpom_rankings_df.loc[updated, 'Team'] = team['sched']
    return kenpom_rankings_df


def main(kenpom):
    print('Starting!')
    print('Generate Kenpom: ' + kenpom)
    tz = pytz.timezone('US/Central')
    # start_date_str = datetime.datetime.now(tz).strftime('%Y%m%d')
    # start_date = datetime.datetime.strptime(start_date_str, '%Y%m%d').date()
    start_date = datetime.datetime.strptime("20231106", '%Y%m%d').date()
    # Set end date to last day of week you want to run for
    end_date = datetime.datetime.strptime("20231112", '%Y%m%d').date()
    print(f'Getting schedule for {start_date} - {end_date}')
    schedule_df = getScheduleForDateRange(start_date, end_date)
    schedule_df = cleanup(schedule_df)
    filename = 'NCAA' + str(start_date) + '-' + str(end_date)
    if kenpom == "True":
        print("Getting Kenpom Rankings")
        kenpom_rankings_df = getKenpomRankings()
        # Create dataframe for away teams
        away_df = kenpom_rankings_df.merge(schedule_df[['Away','Home','Date']], left_on=['Team'], right_on=['Away'], how='inner').rename(columns={'Rk':'Rank','Home':'Opponent'})
        away_df = away_df.merge(kenpom_rankings_df[['Team', 'Rk']], left_on=['Opponent'], right_on=['Team'], how='inner').rename(columns={'Rk':'Opponent Rank'})
        # Create dataframe for home teams
        home_df = kenpom_rankings_df.merge(schedule_df[['Away','Home','Date']], left_on=['Team'], right_on=['Home'], how='inner').rename(columns={'Rk':'Rank','Away':'Opponent'})
        home_df = home_df.merge(kenpom_rankings_df[['Team', 'Rk']], left_on=['Opponent'], right_on=['Team'], how='inner').rename(columns={'Rk':'Opponent Rank'})
        # Combine dataframes
        combined_df = [away_df.loc[:,['Rank','Team_x','Opponent','Opponent Rank','Date']].rename(columns={'Team_x':'Team'}), home_df.loc[:,['Rank','Team_x','Opponent','Opponent Rank','Date']].rename(columns={'Team_x':'Team'})]
        final_df = pd.concat(combined_df)
        final_df['Game Count'] = final_df.groupby('Team')['Team'].transform('count')
        final_df.sort_values(by=['Rank']).to_csv(filename + '.csv',index=False)
        print('Generated file: ' + filename + '.csv')
    else:
        print('Skipping Kenpom and creating schedule file')
        noKPFileName = filename + 'NoKP' + '.csv'
        schedule_df.to_csv(noKPFileName,index=False)
        print('Generated file: ' + noKPFileName)
    print('Done!')

parser=argparse.ArgumentParser(description="Flag that determines if Kenpom rankings are added to result set")
parser.add_argument('--kp', action='store_true', default=False)
args = parser.parse_args()
main(str(args.kp))
