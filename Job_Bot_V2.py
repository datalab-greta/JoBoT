#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 30 14:21:55 2019

@author: maxime castel et cyprien lebrun
"""
import pandas as pd
from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup 



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
dico = {}

url1=0
nbOffre=int(get_nb(soup))



while(url1<=nbOffre):
    """
    premiere boucle qui recupere les urls 1 par 1
    """
    
    url2 = url1+149 if (url1+150<nbOffre) else nbOffre 
    urlR="%d-%d" % (url1, url2)
    
    url='https://candidat.pole-emploi.fr/offres/recherche?lieux=24R&motsCles=python&offresPartenaires=true&range='+urlR+'&rayon=10&tri=0'
    url=simple_get(url)
    
    soup=BeautifulSoup(url, 'html.parser')
    allUrl=get_url(soup)
    url1+=150
     
    for x in allUrl:
        
        """
        Boucle qui recupère toutes les infos par url
        """
        
        liste=[]
        
        raw_html = simple_get(x)
        soup = BeautifulSoup(raw_html, 'html.parser')
    
        title = soup.find("h1").text

        date = soup.find("span", itemprop={"datePosted"}).text
        
        try:           
            secteur = soup.find("span", itemprop={"industry"}).text
        except:
            secteur=''
            
        address = soup.find("span", itemprop={"name"}).text           
        description = soup.find("div", itemprop={"description"}).text
        
        skills = str(soup.find_all("span", itemprop={"skills"})) 
        skills = skills.replace('class="skill-name" itemprop="skills">','').replace('<span','').replace("<span","").replace("</span>","")    
          
        xp = soup.find("span", itemprop={"experienceRequirements"}).text
        
        contrat = soup.find("dd").text

        try:        
            statut = soup.find("span", itemprop={"qualifications"}).text
        except:
            statut = ""
        
        entreprise = str(soup.find_all('h4', {"class":"t4 title"})[0:1])
        entreprise = entreprise[22:len(entreprise)].replace("</h4>]","").replace("\n","")
        
        salaire = str(soup.find_all("span", itemprop={"baseSalary"}))
        
        try:
            idS1=salaire.index('itemprop="unitText"')
            salaire=salaire[idS1:len(salaire)]
            idS1=salaire.index('content=')
            salaire=salaire[idS1:len(salaire)].replace('content="',"")
            if (salaire.find("minValue"))>=0:
                idS2=salaire.index('" itemprop="maxValue"')
                salaire=salaire[0:idS2].replace('" itemprop="minValue"></span><span ',' - ')
            else:
                idS2=salaire.index('"')
                salaire=salaire[0:idS2]
     
        except:
            salaire=""


        partenaire = str(soup.find("a", {"id": "idLienPartenaire"}))
        
        try:
            idx=partenaire.index("href=")
            idP=partenaire.index("id=")
            idO1=partenaire.index("alt=")
            idO2=partenaire.index("src=")
            origin=partenaire[idO1+30:idO2-2]
            partenaire = partenaire[idx:idP].replace('href="','').replace('"','')
            
        except:
            partenaire=""
            origin=""
        ref = str(soup.find_all("span", itemprop={"value"})[0:1])
        ref=ref[24:len(ref)].replace("</span>]","")
        
        listeCol=[title,date,secteur,address,salaire,description,skills,xp,contrat,statut,entreprise,partenaire,origin]
        motclef=""
        z=0      
        for a in listeCol:
            z+=1
            print(z)
            """
            Boucle pour inséré les données et recupérer les mots clef
            """
            liste.append(a)
            a=a.lower()
            print(a)
            print("-")
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
        ajout de chaque liste dans un dico avec la reference en index
        """
        
        dico[ref]=liste

"""
transformation du dictionnaire en df avec les clés en index
"""

df= pd.DataFrame.from_dict(dico,orient=u'index',columns=[u"Intitule",u"Date",u"Secteur_Activite",u"Adresse",u"Salaire",u"Description",u"Competences",u"Experience",u"Type_Contrat",u"Statut",u"Entreprise",u"Lien_Partenaire",u"Site d'origine",u"Mot_Clef"])

"""
Insertion du dico dans la BDD
"""

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
