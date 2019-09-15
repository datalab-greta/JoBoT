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
from sqlalchemy import create_engine, text
import os,configparser
import re
import datetime


config = configparser.ConfigParser()
config.read_file(open(os.path.expanduser("~/.datalab.cnf")))
#
#base = args.base 
#
myBDD="Job_Bot_Cyprien_Maxime?charset=utf8mb4" 
con = create_engine("mysql://%s:%s@%s/%s" % (config['myBDD']['user'], config['myBDD']['password'], config['myBDD']['host'], myBDD))

con.execute("""
CREATE TABLE IF NOT EXISTS `Job_Bot_Cyprien_Maxime`.`AnnoncesPE` (
  
Reference CHAR(7) NOT NULL,
Intitule VARCHAR(200) NULL DEFAULT NULL,
Employeur VARCHAR(250) NULL DEFAULT NULL,
Localite VARCHAR(250) NULL DEFAULT NULL,
Date_MIEL DATE NOT NULL,
Date_MAJ DATE NOT NULL,
Contrat VARCHAR(200) NULL DEFAULT NULL,
Temps_travail VARCHAR(35) NULL DEFAULT NULL,
Salaire VARCHAR(80) NULL DEFAULT NULL,
Savoirs VARCHAR(1000) NULL DEFAULT NULL,
Savoir_etre VARCHAR(1000) NULL DEFAULT NULL,
Qualification VARCHAR(50) NULL DEFAULT NULL,
Expérience VARCHAR(250) NULL DEFAULT NULL,
Formation VARCHAR(100) NULL DEFAULT NULL,
Secteur_activite VARCHAR(200) NULL DEFAULT NULL,
Lien_partenaire VARCHAR(200) NULL DEFAULT NULL,
Provenance VARCHAR(50) NULL DEFAULT NULL,
Contact VARCHAR(250) NULL DEFAULT NULL,	
Description VARCHAR(5000),
PRIMARY KEY (`Reference`))
ENGINE = InnoDB
AUTO_INCREMENT = 0
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_bin;
""")

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

nbOffre=20

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
    
    
        
        try:
            formation = soup.find('span', itemprop={"educationRequirements"}).text
            
        except:
            formation=""
            
        try:    
            savetre = soup.find('h4',class_='t6 skill-subtitle', string='Savoir-être professionnels').next_sibling

            savoir_etre=""
        
            savoir_etre_pro=[]
            
            for i in  savetre.find_all("span", class_="skill-name"):
   
                savoir_etre= i.text
                
                savoir_etre_pro.append(savoir_etre)
                
        except:
            ref
            savoir_etre_pro=[]

        try:
            temps_job = soup.find("dd", itemprop={"workHours"}).text
            #print(temps_job)
        except:
            temps_job = ""
            
        try:
        
            mailto=re.compile("mailto", re.IGNORECASE)
            mail_contact = soup.find("a", href={mailto},class_="",shape="rect").text
            #print(mail_contact)
        except :
            mail_contact =""

        try:
            
            contact_nom = soup.find("div", class_="apply-block").find("dd").text
            #print(contact_nom)
            
        except:
            
            contact_nom =""
            
        contact = contact_nom+mail_contact
        print(contact)
        
            
        listeCol=[title,date,secteur,address,salaire,description,skills,xp,contrat,temps_job,statut,entreprise,partenaire,origin,formation,str(savoir_etre_pro),temps_job,contact]
        motclef=""
        z=0      
        for a in listeCol:
     
            """
            Boucle pour inséré les données et recupérer les mots clef
            """
            liste.append(a)
            a=a.lower()
            
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
        #ajout d'une pause de 5 secondes pour espacer les requêtes. Je ne suis pa sûr de l'emplacement de la commande
        #time.sleep(5)
        """
        ajout de chaque liste dans un dico avec la reference en index
        """
        
        dico[ref]=liste

        delist = ","

#Insertion du dico dans la BDD
      
        param={'ref':ref,'title':title[:200],'employeur':entreprise[:250],'localite':address[:250],'contrat':contrat[:200],'temps_job':temps_job[:35],'salaire':salaire[:80],'savoirs':str(skills).strip('[]')[:1000],'savoir_etre':str(savoir_etre_pro).strip('[]').replace("'","")[:1000],'qualif':statut[:50],'exp':xp[:50],'formation':formation[:100],'secteur':secteur[:200],'lien_tiers':partenaire[:200],'provenance':origin[:50],'contact':contact[:250],'description':description[:5000]}
        s=text("""
INSERT INTO AnnoncesPE(Reference, Intitule, Employeur, Localite, Date_MIEL, Date_MAJ,Contrat,Temps_travail,Salaire,Savoirs,Savoir_etre,Qualification,Expérience,Formation,Secteur_activite,Lien_partenaire,Provenance,Contact,Description)
VALUES(:ref,:title,:employeur,:localite,CURRENT_DATE(), CURRENT_DATE(),:contrat,:temps_job,:salaire,:savoirs,:savoir_etre,:qualif,:exp,:formation,:secteur,:lien_tiers,:provenance,:contact,:description)
ON DUPLICATE KEY
UPDATE
Intitule = :title,
Date_MAJ = CURRENT_DATE()
""")
        con.execute(s,param)


#transformation du dictionnaire en df avec les clés en index

df= pd.DataFrame.from_dict(dico,orient=u'index',columns=[u"Intitule",u"Date",u"Secteur_Activite",u"Adresse",u"Salaire",u"Description",u"Competences",u"Experience",u"Type_Contrat",u"temps_job",u"Statut",u"Entreprise",u"Lien_Partenaire",u"Site d_origine",u"formation",u"savoir_etre_pro",u"temps_job",u"contact",u"Mot_Clef"])

#Backup des annonces ajoutées dans un fichier CSV avec date heure sec au moment de l'import dans la BDD
maintenant = datetime.datetime.now().strftime("_20%y_%m_%d_%HH%M_%S")

autocsv = "/home/cyprien/Documents/jobot/BackUP_AnnoncesPE/BackUpAnnoncesPE"+maintenant+".csv"

df2csv = df.to_csv(autocsv)     







