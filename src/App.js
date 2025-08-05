import logo from './logo.svg';
import './App.css';
import FinopsUserTable from './pages/caludetable';
// import FinOpsDashboard from './pages/dashboard';
import App2 from './pages/app.tsx';
import FinOpsDashboard from './pages/quod.tsx';

function App() {
  return (
    <div>
      <FinOpsDashboard/>
      <App2/>
      <FinopsUserTable/>
    </div>
  );
}

export default App;
