
## Task1
average TF value of applicant for court documents is 0.004398456674562427<br>
average TF value of and for court documents is 0.02042237972494942<br>
average TF value of attack for court documents is 3.7923978178210275e-05<br>
average TF value of protein for court documents is 2.833142937849284e-06<br>
average TF value of court for court documents is 0.0052332310648055495<br>
<br>
average TF value of applicant for wikipedia documents is 5.51239938129749e-06<br>
average TF value of and for wikipedia documents is 0.03344549305657774<br>
average TF value of attack for wikipedia documents is 0.0001973236740154306<br>
average TF value of protein for wikipedia documents is 4.6925279450141374e-05<br>
average TF value of court for wikipedia documents is 0.00039060863250315713<br>
<br>
<br>
<br>
## Task2
('mr', 76.03112736778561)<br>
('applicant', 54.92557629504059)<br>
('relation', 54.942063186960134)<br>
('pty', 84.97584822279718)<br>
('clr', 53.52042057554422)<br>
<br>
<br>
<br>
## Task3
F1: 0.9853528628495338<br>
<br>
three of False Positives:<br>
http://en.wikipedia.org/wiki?curid=19505797<br>
http://en.wikipedia.org/wiki?curid=14683768<br>
http://en.wikipedia.org/wiki?curid=38262801<br>
<br>
As shown in task2, some of words has much higher weights than others. That means, some times, these words made the key decision of whether this document is from Australian court cases or not.<br>
In doc http://en.wikipedia.org/wiki?curid=19505797, the word "applicant" appears 5 times.<br>
In doc http://en.wikipedia.org/wiki?curid=14683768, the word "relation" appears twice.<br>
In doc http://en.wikipedia.org/wiki?curid=38262801, the word "pty" appears 6 times.<br>
The contents of these documents might be similar with the Australian court cases' which also led to wrong decision.<br>
Doc http://en.wikipedia.org/wiki?curid=19505797 is related to the legal system.<br>
Doc http://en.wikipedia.org/wiki?curid=14683768 is related to Australia.<br>
Doc http://en.wikipedia.org/wiki?curid=38262801 is related to the legal system.<br>
