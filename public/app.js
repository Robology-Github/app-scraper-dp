function switchForm(formNumber) {
    const form1 = document.getElementById('form1');
    const form2 = document.getElementById('form2');
    const tabs = document.querySelectorAll('.tab');
    
    tabs.forEach(tab => tab.classList.remove('active'));
    
    if(formNumber === 1) {
      form1.style.display = 'flex';
      form2.style.display = 'none';
      tabs[0].classList.add('active');
      tabs[0].style.backgroundColor = '#DDEA90';
      tabs[1].style.backgroundColor = 'white';
    } else if(formNumber === 2) {
      form1.style.display = 'none';
      form2.style.display = 'flex';
      tabs[1].classList.add('active');
      tabs[1].style.backgroundColor = '#DDEA90';
      tabs[0].style.backgroundColor = 'white';
    }
  }
  


document.addEventListener('DOMContentLoaded', (event) => {
    document.getElementById('searchForm').addEventListener('submit', async function(e) {
        e.preventDefault();
        const backdrop = document.getElementById('loading-backdrop'); // Get the backdrop
        backdrop.style.display = 'block'; // Show the backdrop and spinner
        const searchTerm = document.getElementById('searchTerm').value;
        const numResults = document.getElementById('numResults').value;
        try {
            const response = await fetch(`/search?term=${encodeURIComponent(searchTerm)}&num=${encodeURIComponent(numResults)}`);
            if (!response.ok) throw new Error('Failed to fetch data');
            const data = await response.json();
            showModal('Data successfully scraped!');
        } catch (error) {
            showModal('Error during data fetching: ' + error.message);
        }finally {
            backdrop.style.display = 'block'; // Show the backdrop and spinner
        }
    });
    
      
  });
  

  document.addEventListener('DOMContentLoaded', (event) => {
    document.getElementById('selectForm').addEventListener('submit', async function(e) {
        e.preventDefault();
        const backdrop = document.getElementById('loading-backdrop'); // Get the backdrop
        backdrop.style.display = 'block'; // Show the backdrop and spinner
        const collection = document.getElementById('collectionList').value;
        console.log(collection);
        const country = document.getElementById('countryList').value;
        const collectionNumResults = document.getElementById('collectionNumResults').value; 
        try {
            const response = await fetch(`/collection?collection=${encodeURIComponent(collection)}&country=${encodeURIComponent(country)}&num=${encodeURIComponent(collectionNumResults)}`);
            if (!response.ok) throw new Error('Failed to fetch data');
            const data = await response.json();
            showModal('Data successfully scraped! Check this link for the data: https://storage.googleapis.com/scraper-bucket-1/GooglePlayOutput.csv');
        } catch (error) {
            showModal('Error during data fetching: ' + error.message);
        }finally {
            backdrop.style.display = 'none'; // Hide the backdrop and spinner

        }
    });


    const modal = document.getElementById('modal');
const span = document.getElementsByClassName('close-button')[0];
const modalText = document.getElementById('modal-text');

span.onclick = function() {
    modal.style.display = 'none';
}

window.onclick = function(event) {
    if (event.target == modal) {
        modal.style.display = 'none';
    }
}

function showModal(message) {
    modalText.textContent = message;
    modal.style.display = 'block';
}


});
