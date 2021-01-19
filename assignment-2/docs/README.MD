## Output for task 1-3 
### allDocsAsNumpyArrays.take(3)
[('440693', array([0.08730159, 0.05026455, 0.03174603, ..., 0.        , 0.        ,0.        ])), 
 ('445918', array([0.075179  , 0.03341289, 0.05131265, ..., 0.        , 0.        ,0.        ])), 
 ('447472', array([0.09100529, 0.05238095, 0.02433862, ..., 0.        , 0.        ,0.        ]))]
 
### allDocsAsNumpyArraysTFidf.take(2)
[('2416524', array([0.00632368, 0.00285255, 0.00319641, ..., 0.        , 0.        ,0.        ])), 
 ('2435179', array([0.00770421, 0.0034121 , 0.00400548, ..., 0.        , 0.        ,0.        ]))]

### getPrediction('Sport Basketball Volleyball Soccer', 10)
[('Disambiguation_pages_with_short_description', 2), ('Disambiguation_pages', 2), ('All_disambiguation_pages', 2), ('All_article_disambiguation_pages', 2), ('All_stub_articles', 1), ('1936_in_basketball', 1)]
 
### getPrediction('What is the capital city of Australia?', 10)
[('All_article_disambiguation_pages', 3), 
 ('All_disambiguation_pages', 2), 
 ('Disambiguation_pages_with_short_description', 2), 
 ('Disambiguation_pages', 1), 
 ('Airport_disambiguation', 1), 
 ('Redirects_to_sections', 1)]

### getPrediction('How many goals Vancouver score last year?', 10)
[('Disambiguation_pages_with_short_description', 2), 
 ('Disambiguation_pages', 2), 
 ('All_disambiguation_pages', 2), 
 ('All_article_disambiguation_pages', 2), 
 ('2004â05_in_Central_American_football_leagues', 1), 
 ('2004â05_in_Honduran_football', 1)]



## Output for task 4
### allDocsAsNumpyArrays.take(3)
[Row(DocID='10000591', position_array=[0.06562499701976776, 0.05624999850988388, ..., 0.0]),
 Row(DocID='10007113', position_array=[0.04545454680919647, 0.01515151560306549, ..., 0.0]),
 Row(DocID='10009278', position_array=[0.10526315867900848, 0.017543859779834747, ..., 0.0])]

### allDocsAsNumpyArraysTFidf.take(2)
[Row(DocID='10000591', position_array=[0.006710870191454887, 0.010120031423866749, 0.0034203336108475924, ...0.0]),
 Row(DocID='10007113', position_array=[0.004648221656680107, 0.002725934376940131, 0.0020729294046759605, ...0.0])]
 
### getPrediction('Sport Basketball Volleyball Soccer', 10)
[Row(Category='All_article_disambiguation_pages', count=1), 
 Row(Category='Disambiguation_pages_with_short_description', count=1), 
 Row(Category='All_disambiguation_pages', count=1), 
 Row(Category='Disambiguation_pages', count=1), 
 Row(Category='All_articles_with_unsourced_statements', count=1),  
 Row(Category='Articles_containing_Japanese-language_text', count=1),    
 Row(Category='Articles_using_Infobox_video_game_using_locally_defined_parameters', count=1),   
 Row(Category='Association_football_video_games', count=1), 
 Row(Category='Articles_using_Wikidata_infoboxes_with_locally_defined_images', count=1), 
 Row(Category='1990_video_games', count=1)]

### getPrediction('What is the capital city of Australia?', 10)
[Row(Category='Disambiguation_pages_with_short_description', count=2), 
 Row(Category='Disambiguation_pages', count=2), 
 Row(Category='All_article_disambiguation_pages', count=2), 
 Row(Category='All_disambiguation_pages', count=2), 
 Row(Category='Heterokont_stubs', count=1), 
 Row(Category='Antibiotic_stubs', count=1)]

### getPrediction('How many goals Vancouver score last year?', 10)
[Row(Category='All_article_disambiguation_pages', count=1), 
 Row(Category='All_disambiguation_pages', count=1), 
 Row(Category='Disambiguation_pages', count=1), 
 Row(Category='Disambiguation_pages_with_short_description', count=1), 
 Row(Category='Wikipedia_articles_with_BNF_identifiers', count=1), 
 Row(Category='Articles_needing_translation_from_foreign-language_Wikipedias', count=1),  
 Row(Category='Wikipedia_articles_with_SUDOC_identifiers', count=1), 
 Row(Category='Articles_to_be_expanded_from_April_2012', count=1), 
 Row(Category='1888_births', count=1), 
 Row(Category='Best_Original_Music_Score_Academy_Award_winners', count=1)]

## task 5
### Task 5.1 - Remove Stop Words
No, it would not change the result heavily.

Both training data and testing data would have stop words. Since we have 20k features, it won't all be stop words. The order of distance would not changed a lot because the distance is still calculated by both key words and stopwords. We can consider this in the following formula:
distance between testing and training = key words distance between testing and training + stopwords distance between testing and training
while all stopwords distance are similar, distance can still determined by key words distance.

For example: suppose the k of KNN is 1
with stop words:
distance(testing - training①) = 0.3
distance(testing - training②) = 0.7
the testing result will be same as training① since they are closer.
without stop words:
distance(testing - training①) = 0.4
distance(testing - training②) = 0.6
the testing result will be same as training① since they are closer.

The distance would be changed but the final result won't be changed heavily.

### Task 5.2 - Do English word stemming

Yes, it would change result.
Because all words ["game","gaming","gamed","games"] express the same meaning. It should be treated as one feature.
Imaging a doc are full of this kind of words, the features may be 
|    doc   | game | gaming | gamed | games |
| -------- | ---- | ------ | ----- | ----- |
| training |  1   |    0   |   1   |   0   |
| testing  |  0   |    1   |   0   |   1   |

The training and testing actually describe the same thing. But, the distance of them might be very large.



