import { render } from "@testing-library/react";
import React from 'react';
import Home from './home';

describe('Home', () => {
  test('renders', async () => {
    function onSynchronize() : void {
    }
    const el = render(<Home onSynchronize={onSynchronize}/>);
    expect(el.container.firstChild).toBeTruthy();
  });
});