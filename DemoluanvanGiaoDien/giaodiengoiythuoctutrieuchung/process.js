function fetchdata() {
    const event = document.getElementById('content');
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
        `
        <h2  class="tieude">Các loại bệnh được gợi ý cho ${trieuchung} là:</h2>
        <table id="customers">
        <tr>
        <th>Loại bệnh</th>
        <th>Ratting</th>
        <th>Chức năng</th>
        </tr>
          ${data.map(loaibenh=>{
             return `<tr>
             <td> ${loaibenh.chandoan}</td>
             <td> ${loaibenh.rating}</td>
             <td> <button onclick="fetchdatagoiythuoc('${loaibenh.chandoan}')" id="button">Xem gợi ý thuốc</button> </td>
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


//goi y thuoc
function fetchdatagoiythuoc(loaibenh) {
    const event = document.getElementById('content1');
    fetch(`http://127.0.0.1:5560/ratingforloaibenh?loaibenh=${loaibenh}`)
    .then(response => {
        if(!response.ok){
            throw Error('Error')
        }
        return response.json();
    })
    .then(data=>{
        const html = 
        `
        <h2 class="tieude">Các thuốc được gợi ý cho ${loaibenh} là:</h2>
        <table id="customers">
        <tr>

        <th>Tên thuốc</th>
        <th>Ratting</th>
        
        </tr>
          ${data.map(loaibenh=>{
             return `<tr>
             <td> ${loaibenh.tenhh}</td>
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
function remove1(){
    document.getElementById('content1').innerHTML = " ";   
}