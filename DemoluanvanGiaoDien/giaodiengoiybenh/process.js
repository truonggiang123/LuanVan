function fetchdata() {
    const event = document.getElementById('content');
    if(event!=null)
    {
        remove();
    }
    const trieuchung = document.getElementById('trieuchung').value;
    fetch(`http://127.0.0.1:5560/ratingforuser?trieuchung=${trieuchung}`)
    .then(response => {
        if(!response.ok){
            throw Error('Error')
        }
        return response.json();
    })
    .then(data=>{
        const html = 
        `<table id="customers">
        <tr>
        <th>Loại bệnh</th>
        <th>Ratting</th>
        </tr>
          ${data.map(loaibenh=>{
             return `<tr>
             <td> ${loaibenh.chandoan}</td>
             <td> ${loaibenh.rating}</td>
             </tr>`
          }).join("")}
        </table>`;
        
        event.innerHTML = html
       
    })
    .catch(error=>{
        console.log(error)
    })
}
function remove(){
    document.getElementById('content').innerHTML = " ";
       
}
