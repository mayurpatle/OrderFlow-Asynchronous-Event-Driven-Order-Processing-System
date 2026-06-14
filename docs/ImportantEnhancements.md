## Future Important  Enhancements  

### Session  7 :   
 
When Inventory service was  added  we  build  a consumer that consumes  order.created event and  it checke the  availability of  the  stock if it  is  available  in the StockItemRepository then create a inventory.reserved event and  add the  reservation in the Reservation Entity and  the reservations Items  in the  RservationItem Repo but  if  the  stock are not  available  and the  there  is  event  in the  inventory.failed then thre should be  notifcation service  consuming that and  mailing  the  used initialy our notification service used  to mail after every order created 

### Session  8  :  

Till here we  have    not  added the  actual cost  for  the order  and c we are also not added the actual 
Add the  Actual Cost  of the order
Add Real  Payment Gateway

### Session 9  : 

if the payment fails release the event so that the notificationn service could mail the user regarding the failed event andthe reason why the payment failed



