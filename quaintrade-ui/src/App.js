import React, { useState } from 'react';
import EditableTable from "./EditableTable.js"


import Nav from 'react-bootstrap/Nav';
import Navbar from 'react-bootstrap/Navbar';
import Table from 'react-bootstrap/Table';
import Stack from 'react-bootstrap/Stack';

import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';

import NavDropdown from 'react-bootstrap/NavDropdown';

import Container from 'react-bootstrap/Container';
import Button from 'react-bootstrap/Button';

import './App.css';

const NavBar = () => (
    <Navbar expand="lg" className="bg-body-tertiary" data-bs-theme="dark">
    <Container>
      <Navbar.Brand href="#home">Quaintrade</Navbar.Brand>
      <Navbar.Toggle aria-controls="basic-navbar-nav" />
      <Navbar.Collapse id="basic-navbar-nav">
        <Nav className="me-auto">
          <Nav.Link href="#link">[Service Providers]</Nav.Link>
          <Nav.Link href="#link">[Strategies]</Nav.Link>
          <Nav.Link href="#link">[Back Testing]</Nav.Link>
          <Nav.Link href="#link">[Live Trading]</Nav.Link>
        </Nav>
      </Navbar.Collapse>
    </Container>
  </Navbar>
);

const ServiceProviderTable = () => (
    <Table striped bordered hover>
      <thead>
        <tr>
          <th>ID</th>
          <th>Name</th>
          <th>Class</th>
          <th>Auth</th>
          <th>kwargs</th>
        </tr>
      </thead>
      <tbody>
      </tbody>
    </Table>
);

const ServiceProviderHeaders = ["id", "name", "class",
                                "storage_class", "auth_credentials",
                                "auth_cache_filepath", "custom_kwargs"];

function App() {
  const [showServiceProviders, toggleShowServiceProviders] = useState(true);
  const [serviceProviderData, setServiceProviderData] = useState([{"test1": "v", "test2": "v2"}])
  const [serviceProviderDataHeaders, setServiceProviderDataHeaders] = useState(["test1", "test2"])
    return (
    <Container className="p-3">
        <Container className="p-5 mb-4 bg-dark rounded-3">
            <Stack gap="3">
            <Row>
                <NavBar></NavBar>
            </Row>

            <Row>
                <Col><EditableTable headers={serviceProviderDataHeaders} data={serviceProviderData}/></Col>
                
            </Row>
            </Stack>
        </Container>
    </Container>
    );
};

export default App;
