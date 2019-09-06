#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 30 14:21:55 2019

@author: maxime
"""
import pandas as pd
from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup 
import datetime

url='https://candidat.pole-emploi.fr/offres/recherche?lieux=24R&motsCles=data&offresPartenaires=true&range=0-9&rayon=10&tri=0'

def simple_get(url):
    """
    Se connecte a l'url, si statut = 200 retourne le contenu ( en appelant is_good_response)
    """
    try:
        with closing(get(url, stream=True)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None


def is_good_response(resp):
    """
    Renvoie 200 si connection a l'url
    """
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200 
            and content_type is not None 
            and content_type.find('html') > -1)


def log_error(e):
    """
    retourne l'erreur
    """
    print(e)


def get_url(soup):
    """
    Recupère les urls une par une
    """
    urls = []
    for link in soup.find_all('h2', {'class': 'media-heading'}):
        partial_url = link.a.get('href')
        url = 'https://candidat.pole-emploi.fr' + partial_url
        urls.append(url)
    return urls


"""
stock les url dans une liste
"""
html=simple_get('https://candidat.pole-emploi.fr/offres/recherche?lieux=24R&motsCles=python&offresPartenaires=true&range=0-0&rayon=10&tri=0')
soup= BeautifulSoup(html,'html.parser')
def get_nb(soup):
    nb=soup.find("h1").text
    idxnb=nb.index(' offre')
    nb=nb[0:idxnb]
    return int(nb)
    
listeUrl=[]
listeF = []
dico = {}

url1=0
nbOffre=int(get_nb(soup))
c=0
nbOffre=30

while(url1<=nbOffre):
    c+=1
    print("Liste d'url numero: ",c)
    url2 = url1+149 if (url1+150<nbOffre) else nbOffre 
    urlR="%d-%d" % (url1, url2)
    
    url='https://candidat.pole-emploi.fr/offres/recherche?lieux=24R&motsCles=python&offresPartenaires=true&range='+urlR+'&rayon=10&tri=0'
    url=simple_get(url)
    
    soup=BeautifulSoup(url, 'html.parser')
    allUrl=get_url(soup)
    url1+=150
    
    o=0
    for x in allUrl:
        o+=1
        print('Url numero: ',o)
        """
        boucle qui envoie les urls une par une
        """
        liste=[]
        
        raw_html = simple_get(x)
        soup = BeautifulSoup(raw_html, 'html.parser')
    
        title = soup.find("h1").text

        date = soup.find("span", itemprop={"datePosted"}).text
        
        try:           
            secteur = soup.find("span", itemprop={"industry"}).text
        except:
            secteur='Pas de secteur'
            
        address = soup.find("span", itemprop={"name"}).text           
        description = soup.find("div", itemprop={"description"}).text
        
        skills = str(soup.find_all("span", itemprop={"skills"})) 
        skills = skills.replace('class="skill-name" itemprop="skills">','').replace('<span','').replace("<span","").replace("</span>","")    
          
        xp = soup.find("span", itemprop={"experienceRequirements"}).text
        
        contrat = soup.find("dd").text

        try:        
            statut = soup.find("span", itemprop={"qualifications"}).text
        except:
            statut = "Pas de statut"
        
        entreprise = str(soup.find_all('h4', attrs={"class":"t4 title"})[0:1])
        entreprise = entreprise[22:len(entreprise)].replace("</h4>]","").replace("\n","")
        
        partenaire = str(soup.find("a", {"id": "idLienPartenaire"}))
        
        try:
            idx=partenaire.index("href=")
            idP=partenaire.index("id=")
            partenaire = partenaire[idx:idP].replace('href="','').replace('"','')
    
        except:
            partenaire="Pas de lien"
            
        ref = str(soup.find_all("span", itemprop={"value"})[0:1])
        ref=ref[24:len(ref)].replace("</span>]","")
        
        """
        ajout des elems dans une liste 
        """
        
        listeCol=[title,date,secteur,address,description,skills,xp,contrat,statut,entreprise,partenaire,ref]
        motclef=""
        
        for a in listeCol:
            
            liste.append(a)

            if ((a.find("sql"))>=0 or (a.find("python"))>=0  or (a.find("java"))>=0 or (a.find(" r "))>=0):
                
                if ((a.find("sql"))>=0 ):
                    motclef+="sql "
                if ((a.find("python"))>=0 ):
                    motclef+="python "
                if ((a.find("java"))>=0):
                    motclef+="java "
                if ((a.find(" r "))>=0):
                    motclef+="r "  
            
        liste.append(motclef)
       
        """
        ajout de chaque liste dans une autre liste ( double liste pour faire )
        """
        
        listeF.append(liste)

for i in listeF:
    
    """
    transformation de la liste en dictionnaire
    """
    
    date = str(datetime.datetime.now())  
    dico[date] = i

"""
transformation du dictionnaire en df avec les clés en index
"""

df= pd.DataFrame.from_dict(dico,orient=u'index',columns=[u"Intitule",u"Date",u"Secteur_Activite",u"Adresse",u"Description",u"Competences",u"Experience",u"Type_Contrat",u"Statut",u"Entreprise",u"Lien_Partenaire",u"Reference",u"Mot_Clef"])
print(df)

#
#from sqlalchemy import create_engine
#import argparse
#import os,configparser
#
#parser = argparse.ArgumentParser()
#parser.add_argument("-v", action="store_true", help="Verbose SQL")
#parser.add_argument("--base", help="Répertoire de movies")
#parser.add_argument("--bdd", help="Base de donnée")
#args = parser.parse_args()
#
#config = configparser.ConfigParser()
#config.read_file(open(os.path.expanduser("~/Téléchargements/.datalab.cnf")))
#
#base = args.base 
#
#DB="Job_Bot_Cyprien_Maxime?charset=utf8" 
#con = create_engine("mysql://%s:%s@%s/%s" % (config['myBDD']['user'], config['myBDD']['password'], config['myBDD']['host'], DB), echo=args.v)
#df.to_sql(con=con, name='PE_Scraping', if_exists='append')
