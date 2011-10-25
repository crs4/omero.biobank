Chipal run example
==================

.. todo::

   this is currently a note for ourselves, the idea is to transform it
   in an example of both how to use BIOBANK and various tools cross usage.


In this example, we will show how to do the following.

 * Run a gender check on all the individuals known that have an
   AffymetrixCell DataSample file available.

 * Run a quality check on the same set of DataSets and register the
   results as 'Archetyped' info on the DataSample

 * Create a new group G containing a given number of individuals, with
   an assigned proportion of male/female and affected/control cases
   and such that all the individuals in the group have at least one
   AffymetrixCel DataSample (we could be also filtering on quality).

 * Create a DataCollection containing all the AffymetrixCel
   DataSamples found above.

 * Run Chipal on the latter and save results as GenotypeDataSample
   objects.

 * Subselect subgroups from G by random sampling while keeping the
   same male/female and affected/control ratios.

 * Create DataCollection(s) corresponding to the subgroups.

 * Run Chipal on the latter and save results as GenotypeDataSample
   objects.

 * Do a systematic check on result consistency.


Ilenia writes
-------------


Come vi ho anticipato, ecco di seguito alcune prove che vorrei fare
con Chipal.  Si tratta di prove che avrei voluto fare prima, ma che ho
sempre rimandato perche' il run durava troppo, ma ora che c'e' Chipal
possiamo provare e vedere se si ottiene un risultato qualitativamente
migliore rispetto al run precedente.

 #.  Sarebbe possibile aggiornare i files di libreria listati sotto in
     base ai risultati del vostro allineamento dei probeset sul genoma
     di referenza? ::

       /SHARE/USERFS/bigspace/apt_runs/lib/GenomeWideSNP_6.chrYprobes 
       /SHARE/USERFS/bigspace/apt_runs/lib/GenomeWideSNP_6.chrXprobes 
       /SHARE/USERFS/bigspace/apt_runs/lib/GenomeWideSNP_6.Full.specialSNPs 


     Simone, ricordo che hai confrontato i risultati del vostro
     allineamento con il file che utilizziamo noi::

       /SHARE/USERFS/Genomafs/Data/zara/libfiles_affy_vcf_110301/INFO_SNP_vcf_affy_chrpos_sorted_110421.txt

     A parte quei 28 probeset [N/N] che Peter ha allineato (ma che se
     sono solo 28 potremmo anche decidere di scartare), c'erano altre
     differenze?

 #. Sarebbe possibile aggiornare il file con i parametri del modello,
    nel file sotto, con dati provenienti dal nostro dataset, ossia far
    scrivere a Chipal i parametri del modello?  Birdseed lo fa con
    l'opzione --write-models
    (http://www.affymetrix.com/support/developer/powertools/changelog/apt-probeset-genotype.html),
    non so se in Chipal l'abbiate implementata::

      /SHARE/USERFS/bigspace/apt_runs/lib/GenomeWideSNP_6.birdseed-v2.models 

 #. E' possibile mandare due runs sugli stessi files dell'ultimo run
    che abbiamo mandato, ma separando per malattia, quindi un run con
    controlli + casi di diabete e uno con controlli + casi di
    sclerosi?  Per questo punto non sono necessari i punti precedenti,
    anzi e' meglio un run con gli stessi parametri dell'altro in modo
    da avere risultati confrontabili.  Piu' che altro sarebbe utile
    poter risalire all'affection status dell'individuo a cui e'
    associato ciascun cel file mediante il database, ma probabilmente
    anche questo non e' ancora possibile farlo.  Mi ci e' voluto un po',
    come al solito non e' semplice lavorare con i files e recuparare i
    familiari in famiglie "con buchi", ma ho preparato i seguenti
    elenchi per i due runs da mandare che contengono rispettivamente
    CT+MS+doppia_patologia e relativi familiari, e
    CT+T1D+doppia_patologia e relativi familiari::

      /SHARE/USERFS/Genomafs/Data/zara/liste_chipal_110921/MS_T1D_separate_clusters_111006/MS_CT_ALL.path
      /SHARE/USERFS/Genomafs/Data/zara/liste_chipal_110921/MS_T1D_separate_clusters_111006/T1D_CT_ALL.path

 


Scusate se mi sono dilungata e so anche che siete impegnati con
l'ASHG, manderei io il run se potessi, ma non posso, quindi mi fareste
un grande favore se trovaste un momento per mandarlo voi.

Riassumendo, la prima cosa da fare sarebbe il punto 3: mandare due run
di Chipal sui due elenchi che ho indicato, con gli stessi parametri
dell'ultimo run che abbiamo mandato.  Poi aggiornare i files di
libreria e mandare un run sull'elenco con tutti gli individui (sempre
quello dell'ultimo run) per confrontare i risultati.
